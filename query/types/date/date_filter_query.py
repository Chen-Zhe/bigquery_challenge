from query.sql.backend_factory import get_sql_backend
from query.types.date.utils import *
from errors import *
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)


class SqlDateFilter:
    condition_placeholder = "#DateCondition"
    table_name_prefix = "#TableGroup"
    table_name = table_name_prefix + "({})"
    table_name_regex = table_name_prefix + r"\({}\)"

    def __init__(self, backend_name):
        self.backend = get_sql_backend(backend_name)
        self.tables = self.backend.tables

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

        if tables:
            for group_name, table_list in tables.items():
                query = query.replace(self.table_name.format(group_name),
                                      self.tables.gen_sql_query_tables(group_name, table_list))

            # print(query)
            return self.backend.query(query)
        else:
            # this query won't return anything due to no relevant table. Execute empty query
            return self.backend.query()

    @staticmethod
    def gen_sql_date_range_condition(date_col, date_start, date_end):
        if date_start > date_end:
            raise RequestException("start date must be earlier than end date")
        elif date_start == date_end:
            return f"(date({date_col}) = '{date_start.strftime(supported_date_format)}')"
        else:
            return f"(date({date_col}) >= '{date_start.strftime(supported_date_format)}' " \
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
                # raise RequestException(f"Requested table group '{group_name}' has no relevant table between "
                #                                f"{date_start.strftime(supported_date_format)}"
                #                                f" and {date_end.strftime(supported_date_format)}")
                return dict()

            relevant_tables[group_name] = [table.sql_name for table in relevant_group_tables]

        return relevant_tables
