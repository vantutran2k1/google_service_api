from google.cloud import bigquery
from google.oauth2 import service_account, credentials
from functools import cached_property
from pathlib import Path


class BigqueryClient:
	_SCOPES = [
		"https://www.googleapis.com/auth/cloud-platform",
		"https://www.googleapis.com/auth/drive",
	]

	def __init__(self, billing_project_id: str, sa_file_path: Path = None, adc_file_path: Path = None):
		self._billing_project_id = billing_project_id
		self._sa_file_path = sa_file_path
		self._adc_file_path = adc_file_path

	def _get_service_account_credentials(self):
		return service_account.Credentials.from_service_account_file(
			self._sa_file_path.as_posix(),
			scopes=self.__class__._SCOPES
		)

	def _get_authorized_user_credentials(self):
		return credentials.Credentials.from_authorized_user_file(
			self._adc_file_path.as_posix(), scopes=self.__class__._SCOPES
		)

	@cached_property
	def service(self):
		if self._sa_file_path:
			creds = self._get_service_account_credentials()
			return bigquery.client.Client(credentials=creds, project=self._billing_project_id)
		elif self._adc_file_path:
			creds = self._get_authorized_user_credentials()
			return bigquery.client.Client(credentials=creds, project=self._billing_project_id)
		else:
			raise TypeError("Please provide at least one valid service account or user account info")
