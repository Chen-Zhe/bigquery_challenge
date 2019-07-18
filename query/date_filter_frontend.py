from query.backend_factory import get_sql_backend
from errors import *
from datetime import datetime
import re
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
    except Exception:
        raise RequestException("Invalid date string")


class DataTable:
    def __init__(self, sql_name, date_range_start_string, date_range_end_string):
        self.sql_name = sql_name
        self.date_range_start = validate_date_string(date_range_start_string)
        self.date_range_end = validate_date_string(date_range_end_string)

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


class SqlDateFilter:
    condition_placeholder = "#DateCondition"
    table_name_prefix = "#TableGroup"
    table_name = table_name_prefix + "({})"
    table_name_regex = table_name_prefix + r"\({}\)"

    def __init__(self, backend_name, table_collection):
        self.backend = get_sql_backend(backend_name)
        self.tables = table_collection

    def date_range_query(self, query, date_col, start_date, end_date):
        if not start_date and not end_date:
            # if nothing is specified, query for today
            return self.valid_date_range_query(query, date_col, datetime.now().date())
        elif not end_date:
            # if only start date is specified, query from start date to today
            return self.valid_date_range_query(query, date_col,
                                               validate_date_string(start_date), datetime.now().date())
        elif not start_date:
            # if only end date is specified, query for the single day
            return self.valid_date_range_query(query, date_col, validate_date_string(end_date))
        else:
            date_start = validate_date_string(start_date)
            date_end = validate_date_string(end_date)
            return self.valid_date_range_query(query, date_col, date_start, date_end)

    def date_query(self, query, date_col, date):
        if not date:
            # if nothing is specified, query for today
            return self.valid_date_range_query(query, date_col, datetime.now().date())
        else:
            # if only start date is specified, query from start date to today
            return self.valid_date_range_query(query, date_col, validate_date_string(date))

    def valid_date_range_query(self, query, date_col, date_start, date_end=None):
        if query.find(self.condition_placeholder) == -1:
            raise QueryGenerationException("Incorrect query configuration: date condition filter cannot be inserted")

        if query.find(self.table_name_prefix) == -1:
            raise QueryGenerationException("Incorrect query configuration: table names cannot be inserted")

        if date_end is None:
            date_end = date_start

        query = query.replace(self.condition_placeholder,
                              self.gen_sql_date_range_condition(date_col, date_start, date_end))

        tables = self.get_table_groups(query, date_start, date_end)

        for group_name, table_list in tables.items():
            query = query.replace(self.table_name.format(group_name),
                                  self.tables.gen_sql_query_tables(group_name, table_list))

        # print(query)
        return self.backend.query(query)

    @staticmethod
    def gen_sql_date_range_condition(date_col, date_start, date_end):
        if date_start > date_end:
            raise RequestException("start date must be earlier than end date")
        elif date_start == date_end:
            return f"(date({date_col}) = '{date_start.strftime(supported_date_format)}')"
        else:
            return f"(date({date_col}) >= '{date_start.strftime(supported_date_format)}'" \
                f"AND date({date_col}) <= '{date_end.strftime(supported_date_format)}')"

    def get_table_groups(self, query, date_start, date_end):
        matched_table_names = re.findall(self.table_name_regex.format(r"\S+"), query)

        relevant_tables = dict()

        for table_name in matched_table_names:
            if table_name in relevant_tables:
                continue

            group_name = re.match(self.table_name_regex.format(r"(\S+)"), table_name).group(1)
            relevant_group_tables = self.tables.get_tables(group_name, date_start, date_end)

            if not relevant_group_tables:
                raise RequestException(f"Requested table group '{group_name}' has no relevant table between "
                                               f"{date_start.strftime(supported_date_format)}"
                                               f" and {date_end.strftime(supported_date_format)}")

            relevant_tables[group_name] = [table.sql_name for table in relevant_group_tables]

        return relevant_tables
