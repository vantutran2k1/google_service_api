from pathlib import Path
from google.cloud import bigquery
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.table import Table
from google.cloud.exceptions import NotFound

import pandas as pd

from .BigqueryClient import BigqueryClient


class BigqueryService:
	_TIME_PARTITIONING_TYPE = {
		"HOUR": bigquery.TimePartitioningType.HOUR,
		"DAY": bigquery.TimePartitioningType.DAY,
		"MONTH": bigquery.TimePartitioningType.MONTH,
		"YEAR": bigquery.TimePartitioningType.YEAR
	}

	_WRITE_DISPOSITION = {
		"WRITE_APPEND": bigquery.WriteDisposition.WRITE_APPEND,
		"WRITE_TRUNCATE": bigquery.WriteDisposition.WRITE_TRUNCATE
	}

	def __init__(self, client: BigqueryClient):
		self._service = client.service

	def list_all_dataset_in_project(self, project_id: str) -> list[str]:
		datasets = list(self._service.list_datasets(project=project_id, include_all=False))

		return [dataset.full_dataset_id.replace(":", ".") for dataset in datasets]

	def list_all_table_in_dataset(self, project_id: str, dataset_name: str) -> list[str]:
		dataset = self.get_dataset_by_id(project_id, dataset_name)
		tables = self._service.list_tables(dataset)

		return [table.full_table_id.replace(":", ".") for table in tables]

	def get_dataset_by_id(self, project_id: str, dataset_name: str) -> Dataset:
		dataset_ref = self._get_dataset_ref(project_id=project_id, dataset_name=dataset_name)

		return self._service.get_dataset(dataset_ref=dataset_ref)

	def get_table_by_id(self, table_id: str) -> Table:
		project_id, dataset_name, table_name = self.split_table_id(table_id)
		table_ref = self._get_table_ref(project_id=project_id, dataset_name=dataset_name, table_name=table_name)

		return self._service.get_table(table=table_ref)

	def validate_table_id_does_not_exist(self, table_id: str) -> str:
		if self.check_table_exists(table_id):
			raise ValueError(f"Table with id {table_id} already exists")

		return table_id

	def validate_table_id_exists(self, table_id: str) -> str:
		if not self.check_table_exists(table_id):
			raise ValueError(f"Table with id {table_id} does not exist")

		return table_id

	def get_table_data_into_pandas(self, table_id: str) -> pd.DataFrame:
		table_id = self.validate_table_id_exists(table_id)

		return self.get_data_from_query_into_pandas(f"SELECT * FROM {table_id}")

	def get_data_from_query_into_pandas(self, query_str: str) -> pd.DataFrame:
		query_job = self._service.query(query_str)

		return query_job.result().to_dataframe()

	def insert_rows_into_table(self, table_id: str, insert_rows: list[dict]):
		table_id = self.validate_table_id_exists(table_id)
		self._service.insert_rows_json(table_id, insert_rows)

	def get_data_from_sql_script_file_into_pandas(self, sql_file_path: Path) -> pd.DataFrame:
		with open(sql_file_path, "r") as file:
			query_string = file.read()

		return self.get_data_from_query_into_pandas(query_string)

	def get_table_schema(self, table_id: str) -> dict[str, str]:
		table = self.get_table_by_id(table_id)

		table_schema = dict()
		for col_schema in table.schema:
			table_schema[col_schema.name] = col_schema.field_type

		return table_schema

	def get_table_shape(self, table_id: str) -> tuple[int, int]:
		table_id = self.validate_table_id_exists(table_id)
		table_schema = self.get_table_schema(table_id)
		table = self.get_table_by_id(table_id)

		return table.num_rows, len(table_schema)

	def create_empty_table_with_schema(self, table_id: str, schema_dict: dict[str, str],
	                                   time_partition_type: str = None, partition_col: str = None):
		table_id = self.validate_table_id_does_not_exist(table_id)
		project_id, dataset_name, table_name = self.split_table_id(table_id)
		table_ref = self._get_table_ref(project_id=project_id, dataset_name=dataset_name, table_name=table_name)
		table = bigquery.Table(table_ref=table_ref, schema=self._create_schema_field_list_from_dict(schema_dict))

		if not time_partition_type:
			self._service.create_table(table)
		else:
			table.time_partitioning = self._create_time_partitioning_info(time_partition_type, partition_col)
			self._service.create_table(table)

	def create_table_from_query(self, table_id: str, query_string: str):
		table_id = self.validate_table_id_does_not_exist(table_id)
		job_config = bigquery.QueryJobConfig(destination=table_id)
		query_job = self._service.query(query_string, job_config=job_config)

		query_job.result()

	def create_table_from_csv_file(self, table_id: str, csv_file_path: str, schema_dict: dict[str, str] = None,
	                               time_partition_type: str = None, partition_col: str = None):
		table_id = self.validate_table_id_does_not_exist(table_id)
		job_config_info = {
			"source_format": bigquery.SourceFormat.CSV,
			"allow_jagged_rows": True,
			"allow_quoted_newlines": True
		}

		if schema_dict:
			job_config_info["schema"] = self._create_schema_field_list_from_dict(schema_dict)
			job_config_info["skip_leading_rows"] = 1
		else:
			job_config_info["autodetect"] = True

		if time_partition_type:
			job_config_info["time_partitioning"] = self._create_time_partitioning_info(time_partition_type,
			                                                                           partition_col)

		load_job = self._create_load_csv_job(csv_file_path_str=csv_file_path, job_config_info=job_config_info,
		                                     table_id=table_id)
		load_job.result()
		self._service.get_table(table_id)

	def insert_into_table_from_csv_file(self, table_id: str, csv_file_path: str, write_disposition: str):
		if write_disposition not in self.__class__._WRITE_DISPOSITION:
			raise ValueError(f"Write disposition must be either {', '.join(self.__class__._WRITE_DISPOSITION.keys())}")

		table_id = self.validate_table_id_exists(table_id)
		job_config_info = {
			"source_format": bigquery.SourceFormat.CSV,
			"allow_jagged_rows": True,
			"allow_quoted_newlines": True,
			"skip_leading_rows": 1,
			"write_disposition": self.__class__._WRITE_DISPOSITION.get(write_disposition)
		}

		load_job = self._create_load_csv_job(csv_file_path_str=csv_file_path, job_config_info=job_config_info,
		                                     table_id=table_id)
		load_job.result()
		self._service.get_table(table_id)

	def check_table_exists(self, table_id: str) -> bool:
		try:
			self.get_table_by_id(table_id)
			return True
		except NotFound:
			return False

	def duplicate_table(self, source_table_id: str, destination_table_id: str):
		source_table_id = self.validate_table_id_exists(source_table_id)
		destination_table_id = self.validate_table_id_does_not_exist(destination_table_id)

		job = self._service.copy_table(source_table_id, destination_table_id)
		job.result()

	def delete_table(self, table_id: str):
		table_id = self.validate_table_id_exists(table_id)
		self._service.delete_table(table_id)

	@staticmethod
	def split_table_id(table_id: str) -> tuple[str, str, str]:
		table_part_list = table_id.split(".")
		assert len(table_part_list) == 3, "Table id must have the syntax of {project_id}.{dataset_name}.{table_name}"

		project_id = table_part_list[0]
		dataset_name = table_part_list[1]
		table_name = table_part_list[2]

		return project_id, dataset_name, table_name

	def _get_dataset_ref(self, project_id: str, dataset_name: str):
		return self._service.dataset(dataset_id=dataset_name, project=project_id)

	def _get_table_ref(self, project_id: str, dataset_name: str, table_name: str):
		return self._get_dataset_ref(project_id=project_id, dataset_name=dataset_name).table(table_id=table_name)

	@staticmethod
	def _construct_table_id(project_id: str, dataset_name: str, table_name: str) -> str:
		return f"{project_id}.{dataset_name}.{table_name}"

	@staticmethod
	def _create_schema_field_list_from_dict(schema_dict: dict[str, str]) -> list[bigquery.SchemaField]:
		schema_info_list: list[bigquery.SchemaField] = []
		for col_name, col_type in schema_dict.items():
			schema_info_list.append(bigquery.SchemaField(name=col_name, field_type=col_type))

		return schema_info_list

	def _create_time_partitioning_info(self, time_partition_type: str, partition_col: str = None):
		if time_partition_type not in self.__class__._TIME_PARTITIONING_TYPE:
			raise ValueError(
				f"Time partition type must be either {', '.join(self.__class__._TIME_PARTITIONING_TYPE.keys())}")

		table_time_partitioning_info: dict = {"type_": self.__class__._TIME_PARTITIONING_TYPE.get(time_partition_type)}
		if partition_col:
			table_time_partitioning_info["field"] = partition_col

		return bigquery.TimePartitioning(**table_time_partitioning_info)

	def _create_load_csv_job(self, csv_file_path_str: str, job_config_info: dict, table_id: str):
		if csv_file_path_str[:3] == "gs:":
			load_job = self._service.load_table_from_uri(csv_file_path_str, table_id,
			                                             job_config=bigquery.LoadJobConfig(**job_config_info))
		else:
			with open(csv_file_path_str, "rb") as source_file:
				load_job = self._service.load_table_from_file(source_file, table_id,
				                                              job_config=bigquery.LoadJobConfig(**job_config_info))

		return load_job
