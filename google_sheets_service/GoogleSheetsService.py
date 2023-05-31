import re
from typing import Optional

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from openpyxl.utils.cell import column_index_from_string, get_column_letter

from .GoogleSheetsClient import GoogleSheetsClient


class GoogleSheetsService:
    def __init__(self, client: GoogleSheetsClient):
        self._service = client.service

    def open_spreadsheet(self, spreadsheet_key: str) -> gspread.Spreadsheet:
        return self._service.open_by_key(key=spreadsheet_key)

    def create_spreadsheet(self, spreadsheet_name: str, parent_folder_id: str = None) -> gspread.Spreadsheet:
        return self._service.create(title=spreadsheet_name, folder_id=parent_folder_id)

    def write_df_to_sheet(self, worksheet: gspread.Worksheet, df_to_write: pd.DataFrame, starting_cell: str = "A1",
                          include_header: bool = True) -> None:

        start_row_int, start_col_int = self.extract_row_and_column_int_from_string(
            starting_cell)
        set_with_dataframe(worksheet, df_to_write, row=start_row_int, col=start_col_int,
                           include_column_header=include_header)

    def read_data_from_sheet_to_pandas(self, worksheet: gspread.Worksheet, starting_cell_str: str = "A1",
                                       end_cell_row: int = None, end_cell_col_str: str = None,
                                       include_header: bool = True):
        data: list[list[str]] = worksheet.get_all_values()
        if len(data) == 0 or len(data[0]) == 0:
            return

        starting_cell_row, starting_cell_col = self.extract_row_and_column_int_from_string(
            starting_cell_str)

        if (not end_cell_row) or (end_cell_row > len(data)):
            end_cell_row = len(data)

        if (not end_cell_col_str) or (
                self.extract_row_and_column_int_from_string(end_cell_col_str)[1] > len(data[0])):
            end_cell_col: int = len(data[0])
        else:
            end_cell_col: int = self.extract_row_and_column_int_from_string(end_cell_col_str)[
                1]

        if include_header:
            if len(data) == 1:
                return pd.DataFrame(columns=data[0])

            header_range_str: str = f"R{starting_cell_row}C{starting_cell_col}:R{starting_cell_row}C{end_cell_col}"
            data_range_str: str = f"R{starting_cell_row + 1}C{starting_cell_col}:R{end_cell_row}C{end_cell_col}"

            try:
                columns_list: list[str] = worksheet.get(header_range_str)[0]
            except IndexError:
                columns_list: list[str] = [""] * \
                    (end_cell_col - starting_cell_col + 1)

            df_values = worksheet.get(data_range_str)

            columns_list = self._fill_columns_list(
                columns_list, end_cell_col - starting_cell_col + 1)

            if len(columns_list) > max([len(row) for row in df_values]):
                columns_list = columns_list[:max(
                    [len(row) for row in df_values])]

            return pd.DataFrame(data=df_values, columns=columns_list).replace("", None)

        else:
            data_range_str: str = f"R{starting_cell_row}C{starting_cell_col}:R{end_cell_row}C{end_cell_col}"

            df_values = worksheet.get(data_range_str)

            return pd.DataFrame(data=df_values)

    @staticmethod
    def extract_row_and_column_int_from_string(cell_string: str) -> tuple[Optional[int], Optional[int]]:

        try:
            row_value: int = int("".join(re.findall(r"[0-9]*$", cell_string)))
        except ValueError:
            row_value = None

        try:
            col_value: int = column_index_from_string(
                "".join(re.findall(r"^[a-zA-z]*", cell_string)))
        except ValueError:
            col_value = None

        return row_value, col_value

    @staticmethod
    def extract_column_string_from_string(cell_string: str) -> str:
        return "".join(re.findall(r"^[a-zA-z]*", cell_string))

    @staticmethod
    def convert_column_int_to_string(col_int: int) -> str:
        return get_column_letter(col_int)

    @staticmethod
    def _fill_columns_list(columns_list: list[str], list_length: int) -> list[str]:
        count: int = 0
        for i in range(list_length):
            try:
                if columns_list[i] == "":
                    columns_list[i] = "column" + str(count)
                    count += 1
            except IndexError:
                columns_list.append("column" + str(count))
                count += 1

        return columns_list
