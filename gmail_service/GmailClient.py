from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from functools import cached_property
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import os

load_dotenv(find_dotenv())


class GmailClient:
	_SCOPES = ["https://mail.google.com/"]
	_SERVICE_NAME = "gmail"
	_SERVICE_VERSION = "v1"

	def __init__(self, credentials_path: Path, gmail_token_path: Path):
		self._credentials_path = credentials_path
		self._gmail_token_path = gmail_token_path

	@cached_property
	def service(self):
		creds = None
		# The file token.json stores the user's access and refresh tokens, and is
		# created automatically when the authorization flow completes for the first
		# time.
		if os.path.exists(self._gmail_token_path):
			creds = Credentials.from_authorized_user_file(self._gmail_token_path.as_posix(), self.__class__._SCOPES)
		# If there are no (valid) credentials available, let the user log in.
		if not creds or not creds.valid:
			if creds and creds.expired and creds.refresh_token:
				creds.refresh(Request())
			else:
				flow = InstalledAppFlow.from_client_secrets_file(self._credentials_path.as_posix(), self.__class__._SCOPES)
				creds = flow.run_local_server(port=0)
			# Save the credentials for the next run
			with open(self._gmail_token_path, "w") as token:
				token.write(creds.to_json())

		return build(self.__class__._SERVICE_NAME, self.__class__._SERVICE_VERSION, credentials=creds)


if __name__ == "__main__":
	service = GmailClient(
		credentials_path=Path(os.environ.get("GOOGLE_CREDENTIALS_PATH")),
		gmail_token_path=Path(os.environ.get("GMAIL_TOKEN_PATH"))
	).service
