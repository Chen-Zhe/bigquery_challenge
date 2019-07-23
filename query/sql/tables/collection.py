import logging

from errors import ServerException
from query.types.date.utils import DateFormat as D


class DataTable:
    def __init__(self, sql_name, date_range_start_string, date_range_end_string):
        self.sql_name = sql_name
        self.date_range_start = D.validate_date_string(date_range_start_string)
        self.date_range_end = D.validate_date_string(date_range_end_string)

        if self.date_range_start > self.date_range_end:
            raise ServerException(f"Table '{sql_name}' has incorrectly-configured date range")


class SqlTableCollection:
    def __init__(self, table_group_names):
        self.tables = {group: list() for group in table_group_names}

    def register_table(self, group_name, data_table):
        if group_name not in self.tables:
            logging.error(f"group {group_name} does not exist in this table collection")
            return

        if data_table.date_range_start > data_table.date_range_end:
            logging.error(f"sql table {data_table.sql_name} has an invalid date range")
            return

        self.tables[group_name].append(data_table)
        self.tables[group_name].sort(key=lambda x: x.date_range_start)

    def get_tables(self, group_name, date_start, date_end):
        relevant_tables = list()

        if group_name not in self.tables:
            logging.error(f"group {group_name} does not exist in this table collection")
            return relevant_tables

        registered_tables = self.tables[group_name]

        for table in registered_tables:
            if table.date_range_start <= date_end and date_start <= table.date_range_end:
                relevant_tables.append(table)

        return relevant_tables

    @staticmethod
    def gen_sql_query_tables(group_name, table_name_list):
        return f"({' UNION ALL '.join([f'SELECT * FROM `{name}`' for name in table_name_list])}) {group_name}"
