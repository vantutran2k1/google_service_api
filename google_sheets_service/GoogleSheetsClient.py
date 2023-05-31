from __future__ import print_function
from functools import cached_property
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import os
import gspread

load_dotenv(find_dotenv())


class GoogleSheetsClient:
	def __init__(self, sa_file_path: Path = None, credentials_path: Path = None, sheets_token_path: Path = None):
		self._sa_file_path = sa_file_path
		self._credentials_path = credentials_path
		self._sheets_token_path = sheets_token_path

	@cached_property
	def service(self):
		if self._sa_file_path:
			return gspread.service_account(filename=self._sa_file_path.as_posix())
		elif self._credentials_path and self._sheets_token_path:
			return gspread.oauth(
				credentials_filename=self._credentials_path.as_posix(),
				authorized_user_filename=self._sheets_token_path.as_posix()
			)
		else:
			raise TypeError("Please provide at least one valid service account or user account info")


if __name__ == "__main__":
	service = GoogleSheetsClient(
		credentials_path=Path(os.environ.get("GOOGLE_CREDENTIALS_PATH")),
		sheets_token_path=Path(os.environ.get("GG_SHEETS_TOKEN_PATH"))
	).service
