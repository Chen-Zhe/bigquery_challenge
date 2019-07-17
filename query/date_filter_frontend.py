from query.backend_factory import get_sql_backend
from errors import *
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

supported_date_format = "%Y-%m-%d"

def validate_date_string(date_string):
    """
    Check if the given date string conforms to the required ISO format (YYYY-MM-DD). Raise exception if it doesn't
    :param date_string: string representing a date
    :return: Date object
    """

    try:
        return datetime.strptime(date_string, supported_date_format).date()
    except ValueError as e:
        raise RequestException(f"Incorrect date format in {date_string} resulting in {str(e)}")


class DataTable:
    def __init__(self, sql_name, date_range_start_string, date_range_end_string):
        self.sql_name = sql_name
        self.date_range_start = validate_date_string(date_range_start_string)
        self.date_range_end = validate_date_string(date_range_end_string)


class TableCollection:
    def __init__(self, table_group_names):
        self.tables = {group: list() for group in table_group_names}

    def register_table(self, group_name, data_table):
        if group_name not in self.tables:
            logging.error(f"group {group_name} does not exist in this table collection")
            return
        
        self.tables[group_name].append(data_table)
        self.tables[group_name].sort(key=lambda x: x.date_range_start)

    def get_tables(self, group_name, start_date, end_date):
        if group_name not in self.tables:
            logging.error(f"group {group_name} does not exist in this table collection")
            return

        registered_tables = self.tables[group_name]





class DateFilterFrontend:

    condition_placeholder = "$DateCondition"

    def __init__(self, backend_name):
        self.backend = get_sql_backend(backend_name)

    def date_range_query(self, query, date_col, start_date, end_date):

        if start

        start_date = validate_date_string(start_date)
        end_date = validate_date_string(end_date)
        
        if query.find(self.condition_placeholder) != -1:
            query = query.replace(self.condition_placeholder,
                f"date({date_col}) >= '{start_date.strftime(supported_date_format)}' AND date({date_col}) <= '{end_date.strftime(supported_date_format)}'")
            return self.backend.query(query)
        else:
            raise ServerException("Incorrect query configuration")

    def date_query(self, query, date_col, date):
        pass

    def valid_date_range_query(self, date_start, date_end):
        pass
