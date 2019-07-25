from errors import ServerException
from query.types.date.utils import DateFormat as D


class DataTable:
    """
    Storage model for a table storing data within a certain date range
    """
    def __init__(self, sql_name, date_range_start_string, date_range_end_string):
        self.sql_name = sql_name
        self.date_range_start = D.validate_date_string(date_range_start_string)
        self.date_range_end = D.validate_date_string(date_range_end_string)

        if self.date_range_start > self.date_range_end:
            raise ServerException(f"Table '{sql_name}' has incorrectly-configured date range")


class SqlTableCollection:
    """
    A collection of SQL tables for SqlDateFilter to pick the relevant tables from
    """
    def __init__(self, table_group_names):
        self.groups = table_group_names
        self.tables = {group: list() for group in table_group_names}

    def register_table(self, group_name, data_table):
        """
        Register a table into the collection
        :param group_name: name of a group of tables with the same schema but different data ranges
        :param data_table: corresponding DataTable entry
        :return: nothing
        """
        if group_name not in self.tables:
            raise ServerException(f"group '{group_name}' does not exist in this table collection")

        if data_table.date_range_start > data_table.date_range_end:
            raise ServerException(f"sql table '{data_table.sql_name}' has an invalid date range")

        self.tables[group_name].append(data_table)
        self.tables[group_name].sort(key=lambda x: x.date_range_start)

    def get_tables(self, group_name, date_start, date_end):
        """
        Retrieve relevant tables in this date range
        :param group_name: table group name
        :param date_start: start of the date range (inclusive)
        :param date_end: end of the date range (inclusive)
        :return: list of DataTable
        """
        relevant_tables = list()

        if group_name not in self.tables:
            raise ServerException(f"group {group_name} does not exist in this table collection")

        registered_tables = self.tables[group_name]

        # TODO: improve performance through binary search
        # TODO: allow intake of multiple data range pairs
        for table in registered_tables:
            if table.date_range_start <= date_end and date_start <= table.date_range_end:
                relevant_tables.append(table)

        return relevant_tables

    @staticmethod
    def gen_sql_query_tables(group_name, table_list):
        """
        Generate SQL query to union relevant tables for processing
        :param group_name: group name for the full table for SQL query use
        :param table_list: list of DataTables
        :return: SQL query that can be nested
        """
        return f"({' UNION ALL '.join([f'SELECT * FROM `{table.sql_name}`' for table in table_list])}) {group_name}"
