from pathlib import Path
import io

from .GcsClient import GcsClient


class GcsService:
	def __init__(self, client: GcsClient, bucket_name: str):
		self._service = client.service
		self._bucket = self._service.bucket(bucket_name=bucket_name)

	def get_direct_children_folders(self, prefix: str) -> list[str]:
		prefix = self._add_slash_to_prefix(prefix)
		blobs = self._service.list_blobs(self._bucket, prefix=prefix)

		direct_children_folders: list[str] = []
		for blob in blobs:
			if blob.name != prefix and blob.name.endswith("/") and len(blob.name.split("/")) == len(
					prefix.split("/")) + 1:
				direct_children_folders.append(blob.name.split("/")[-2])

		return direct_children_folders

	def get_direct_children_files(self, prefix: str) -> list[str]:
		prefix = self._add_slash_to_prefix(prefix)
		blobs = self._service.list_blobs(self._bucket, prefix=prefix)

		direct_children_files: list[str] = []
		for blob in blobs:
			if not blob.name.endswith("/") and len(blob.name.split("/")) == len(prefix.split("/")):
				direct_children_files.append(blob.name.split("/")[-1])

		return direct_children_files

	def upload_file(self, file_path: Path, prefix: str) -> str:
		prefix = self._add_slash_to_prefix(prefix)

		blob_name = prefix + file_path.name
		blob = self._bucket.blob(blob_name)
		blob.upload_from_filename(file_path)

		return blob_name

	def create_folder_if_not_exists(self, prefix: str, folder_name: str) -> str:
		prefix = self._add_slash_to_prefix(prefix)

		blob_name = prefix + folder_name + "/"
		blob = self._bucket.blob(blob_name)
		if not blob.exists():
			blob.upload_from_string("")

		return blob_name

	def download_blob(self, blob_name: str, file_path: Path) -> Path:
		blob = self._bucket.blob(blob_name)
		blob.download_to_filename(file_path)

		return file_path

	def read_blob(self, blob_name: str) -> io.BytesIO:
		blob = self._bucket.blob(blob_name)

		return io.BytesIO(blob.download_as_bytes())

	def delete_blob(self, blob_name: str):
		blobs = list(self._bucket.list_blobs(prefix=blob_name))
		self._bucket.delete_blobs(blobs)

	@staticmethod
	def _add_slash_to_prefix(prefix: str) -> str:
		if not prefix.endswith("/"):
			prefix += "/"

		return prefix
