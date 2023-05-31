from __future__ import annotations

import base64
import os
from base64 import urlsafe_b64decode, urlsafe_b64encode
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from mimetypes import guess_type as guess_mime_type
from pathlib import Path
from datetime import datetime
from typing import Optional
from io import BytesIO
from loguru import logger
from pydantic import ValidationError

import msoffcrypto
import pandas as pd

from .GmailClient import GmailClient
from .gmail_custom_types.GmailMessage import GmailMessage
from .gmail_custom_types.GmailThread import GmailThread
from .gmail_custom_types.AttachmentIdNamePair import AttachmentIdNamePair


class GmailService:
	def __init__(self, client: GmailClient):
		self._service = client.service

	def send_email_message(self, from_email, destination_list, subject, body, body_type: str = "plain",
	                       attachments: list = None,
	                       thread_id: str = None, cc_list: list[str] = None, bcc_list: list[str] = None):
		return self._service.users().messages().send(
			userId="me",
			body=self._build_message(from_email, destination_list, subject, body, body_type, attachments, thread_id,
			                         cc_list, bcc_list)
		).execute()

	def search_messages_by_query(self, query: str) -> list[GmailMessage]:
		"""Follow the link: https://support.google.com/mail/answer/7190 to get more information about querying emails"""
		result = self._service.users().messages().list(userId="me", q=query).execute()

		messages = self._parse_messages_dict(result)

		while "nextPageToken" in result:
			page_token = result["nextPageToken"]
			result = self._service.users().messages().list(userId="me", q=query, pageToken=page_token).execute()

			messages.extend(self._parse_messages_dict(result))

		return messages

	def get_thread_by_id(self, thread_id: str) -> GmailThread:
		result = self._service.users().threads().get(userId="me", id=thread_id).execute()

		try:
			return GmailThread.parse_obj(result)
		except ValidationError:
			raise FileExistsError(f"Thread with id {thread_id} does not exist")

	def get_message_by_id(self, message_id: str) -> Optional[GmailMessage]:
		result = self._service.users().messages().get(userId="me", id=message_id).execute()

		try:
			return GmailMessage.parse_obj(result)
		except ValidationError:
			return None

	@staticmethod
	def query_attachments_from_email_message(message: GmailMessage, attachment_name_filter: list[str] = None,
	                                         attachment_ex_filter: str = None) -> list[AttachmentIdNamePair]:
		attachment_list: list[AttachmentIdNamePair] = []

		if message.payload.parts:
			for part in message.payload.parts:
				if part.filename != "":
					if ((attachment_name_filter is None) or (part.filename in attachment_name_filter)) and (
							(attachment_ex_filter is None) or (part.filename.split(".")[-1] == attachment_ex_filter)):
						attachment_pair = AttachmentIdNamePair(messageId=message.id,
						                                       attachmentId=part.body.attachmentId,
						                                       attachment_file_name=part.filename)

						attachment_list.append(attachment_pair)

		return attachment_list

	def download_attachments(
			self,
			attachment_list: list[AttachmentIdNamePair],
			parent_folder: Path = Path("."),
			rename_mapping_dict: dict[str, str] = None
	) -> list[Path]:
		attachment_full_path_list: list[Path] = []

		for attachment_pair in attachment_list:
			data = self._get_data_from_attachment_pair(attachment_pair)

			if rename_mapping_dict:
				try:
					attachment_path: Path = (
							parent_folder / rename_mapping_dict[attachment_pair.attachment_file_name]
					)
				except KeyError:
					attachment_path: Path = parent_folder / attachment_pair.attachment_file_name
			else:
				attachment_path: Path = parent_folder / attachment_pair.attachment_file_name

			if data:
				with open(attachment_path, "wb") as f:
					f.write(urlsafe_b64decode(data))
					attachment_full_path_list.append(attachment_path.resolve())
					logger.info(f"Downloaded file {attachment_path.resolve()}")

		return attachment_full_path_list

	def read_csv_attachment_to_pandas(self, attachment_pair: AttachmentIdNamePair) -> pd.DataFrame:
		assert attachment_pair.attachment_file_name.split(".")[-1] == "csv"

		data: str = self._get_data_from_attachment_pair(attachment_pair)
		df: pd.DataFrame = pd.read_csv(BytesIO(base64.urlsafe_b64decode(data.encode("UTF-8"))))

		return df

	def read_excel_attachment_to_pandas(self, attachment_pair: AttachmentIdNamePair, password: str = None,
	                                    sheet_name: str | list = None) -> pd.DataFrame:
		assert attachment_pair.attachment_file_name.split(".")[-1] == "xlsx"

		data: str = self._get_data_from_attachment_pair(attachment_pair)

		if not password:
			return pd.read_excel(BytesIO(base64.urlsafe_b64decode(data.encode("UTF-8"))), sheet_name=sheet_name,
			                     dtype=str)

		temp = BytesIO()
		excel = msoffcrypto.OfficeFile(BytesIO(base64.urlsafe_b64decode(data.encode("UTF-8"))))
		excel.load_key(password)
		excel.decrypt(temp)

		return pd.read_excel(temp, sheet_name=sheet_name, dtype=str)

	def _parse_messages_dict(self, messages_dict: dict) -> list[GmailMessage]:
		messages: list[GmailMessage] = []

		if "messages" in messages_dict:
			for message in messages_dict["messages"]:
				parsed_message: GmailMessage = GmailMessage.parse_obj(message)
				gmail_message: GmailMessage = self.get_message_by_id(message_id=parsed_message.id)
				if gmail_message:
					messages.append(gmail_message)

		return messages

	def _build_message(self, from_email, destination_list, subject, body, body_type: str, attachments: list = None,
	                   thread_id: str = None,
	                   cc_list: list[str] = None, bcc_list: list[str] = None):
		def __add_info_to_message(_message):
			_message["To"] = ", ".join(destination_list)
			_message["From"] = from_email
			_message["Subject"] = subject

			if cc_list:
				_message["Cc"] = ", ".join(cc_list)

			if bcc_list:
				_message["Bcc"] = ", ".join(bcc_list)

		if not attachments:  # no attachments given
			message = MIMEText(body, body_type)
			__add_info_to_message(message)
		else:
			message = MIMEMultipart()
			__add_info_to_message(message)
			message.attach(MIMEText(body, body_type))

			for filename in attachments:
				self._add_attachment(message, filename)

		if thread_id:
			return {"raw": urlsafe_b64encode(message.as_bytes()).decode(), "threadId": thread_id}
		else:
			return {"raw": urlsafe_b64encode(message.as_bytes()).decode()}

	def _get_data_from_attachment_pair(self, attachment_pair: AttachmentIdNamePair) -> str:
		attachment = self._service.users().messages().attachments().get(id=attachment_pair.attachmentId, userId="me",
		                                                                messageId=attachment_pair.messageId).execute()

		return attachment["data"]

	@staticmethod
	def convert_unix_timestamp_to_datetime(unix_time_value: str, scale: int = 1000) -> datetime:
		return datetime.fromtimestamp(int(unix_time_value) / scale)

	# Adds the attachment with the given filename to the given message
	@staticmethod
	def _add_attachment(message, filename):
		content_type, encoding = guess_mime_type(filename)
		if content_type is None or encoding is not None:
			content_type = "application/octet-stream"

		main_type, sub_type = content_type.split("/", 1)

		if main_type == "text":
			fp = open(filename, "rb")
			msg = MIMEText(fp.read().decode(), _subtype=sub_type)
			fp.close()

		elif main_type == "image":
			fp = open(filename, "rb")
			msg = MIMEImage(fp.read(), _subtype=sub_type)
			fp.close()

		elif main_type == "audio":
			fp = open(filename, "rb")
			msg = MIMEAudio(fp.read(), _subtype=sub_type)
			fp.close()

		else:
			fp = open(filename, "rb")
			msg = MIMEBase(main_type, sub_type)
			msg.set_payload(fp.read())
			fp.close()

		filename = os.path.basename(filename)
		msg.add_header("Content-Disposition", "attachment", filename=filename)
		message.attach(msg)
