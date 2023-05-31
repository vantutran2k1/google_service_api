import os.path
from pathlib import Path
from googleapiclient.http import MediaIoBaseDownload

from .GoogleDriveClient import GoogleDriveClient


class GoogleDriveService:
	def __init__(self, client: GoogleDriveClient):
		self._service = client.service

	def upload_file_to_drive(self, file_path: Path, parent_folder_id: str = None, file_name: str = None):
		file_metadata = {
			"name": file_name if file_name else file_path.name,
			"parents": [parent_folder_id] if parent_folder_id else None
		}
		media = self._service.files().create(
			body=file_metadata,
			media_body=file_path.as_posix(),
			fields="id",
			supportsAllDrives=True
		).execute()

	def upload_folder_to_drive(self, folder_path: Path, parent_folder_id: str = None):
		folder_metadata = {
			"name": folder_path.name,
			"parents": [parent_folder_id] if parent_folder_id else None,
			"mimeType": "application/vnd.google-apps.folder"
		}
		folder_id = self._service.files().create(
			body=folder_metadata,
			fields="id",
			supportsAllDrives=True
		).execute().get("id")

		for file_path in folder_path.iterdir():
			if file_path.is_dir():
				self.upload_folder_to_drive(folder_path=file_path, parent_folder_id=folder_id)
			else:
				self.upload_file_to_drive(file_path=file_path, parent_folder_id=folder_id)

	def download_file_from_drive(self, file_id: str, parent_directory: Path):
		file_name = self._get_obj_by_id(file_id=file_id).get("name")
		file_path = parent_directory / file_name

		if os.path.exists(file_path):
			file_path = Path(f"{file_path.parent}/{file_path.stem} (1){file_path.suffix}")

		request = self._service.files().get_media(fileId=file_id, supportsAllDrives=True)
		fh = file_path.open("wb")
		downloader = MediaIoBaseDownload(fh, request)
		done = False
		while not done:
			status, done = downloader.next_chunk()

	def download_folder_from_drive(self, folder_id: str, parent_directory: Path):
		folder_name = self._get_obj_by_id(file_id=folder_id).get("name")
		folder_path = parent_directory / folder_name

		if os.path.exists(folder_path):
			folder_path = Path(f"{folder_path.parent}/{folder_path.stem} (1){folder_path.suffix}")
		folder_path.mkdir()

		obj_ids = [obj["id"] for obj in self._list_objects_in_folder(parent_id=folder_id)]
		for obj_id in obj_ids:
			if self._get_obj_by_id(file_id=obj_id).get("mimeType") == "application/vnd.google-apps.folder":
				self.download_folder_from_drive(folder_id=obj_id, parent_directory=folder_path)
			else:
				self.download_file_from_drive(file_id=obj_id, parent_directory=folder_path)

	def list_file_names(self, parent_folder_id: str):
		return [obj["name"] for obj in self._list_objects_in_folder(parent_id=parent_folder_id) if
		        obj["mimeType"] != "application/vnd.google-apps.folder"]

	def list_folder_names(self, parent_folder_id: str):
		return [obj["name"] for obj in self._list_objects_in_folder(parent_id=parent_folder_id) if
		        obj["mimeType"] == "application/vnd.google-apps.folder"]

	def _list_objects_in_folder(self, parent_id: str = None, object_name: str = None):
		query = f"'{parent_id}' in parents and trashed=false" if parent_id else ""
		query += f" and name='{object_name}'" if object_name else ""
		response = self._service.files().list(
			q=query,
			fields="nextPageToken, files",
			supportsAllDrives=True,
			includeItemsFromAllDrives=True
		).execute()
		return response.get("files", [])

	def _get_obj_by_id(self, file_id: str):
		return self._service.files().get(fileId=file_id, supportsAllDrives=True).execute()
