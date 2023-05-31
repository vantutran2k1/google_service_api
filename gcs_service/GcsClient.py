from google.cloud import storage
from functools import cached_property
from pathlib import Path


class GcsClient:
	def __init__(self, sa_file_path: Path):
		self._sa_file_path = sa_file_path

	@cached_property
	def service(self):
		return storage.Client.from_service_account_json(self._sa_file_path.as_posix())
