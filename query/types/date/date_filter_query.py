import re
from datetime import datetime

from cache.strategy.date_range import DateRangeCache
from errors import *
from query.sql.backend_factory import get_sql_backend
from query.types.date.utils import DateFormat as D


class SqlDateFilter:
    condition_placeholder = "#DateCondition"
    table_name_prefix = "#TableGroup"
    table_name = table_name_prefix + "({})"
    table_name_regex = table_name_prefix + r"\({}\)"

    def __init__(self, backend_name, query_name):
        self.backend = get_sql_backend(backend_name)
        self.tables = self.backend.tables
        self.cache = DateRangeCache(query_name + str(backend_name))
        self.date_range_pairs = None

    def set_date_range(self, start_date, end_date):
        if not start_date and not end_date:
            # if nothing is specified, query for today
            self.validate_date_range(datetime.now().date())
        elif not end_date:
            # if only start date is specified, query from start date to today
            self.validate_date_range(D.validate_date_string(start_date), datetime.now().date())
        elif not start_date:
            # if only end date is specified, query for the single day
            self.validate_date_range(D.validate_date_string(end_date))
        else:
            date_start = D.validate_date_string(start_date)
            date_end = D.validate_date_string(end_date)
            self.validate_date_range(date_start, date_end)

    def set_date(self, date):
        if not date:
            # if nothing is specified, query for today
            date = datetime.now().date()
        else:
            # if only start date is specified, query from start date to today
            date = D.validate_date_string(date)

        if self.cache.is_date_cached(date):
            self.date_range_pairs = list()
        else:
            self.date_range_pairs = [(date, date)]

    def validate_date_range(self, date_start, date_end=None):
        if date_end is None:
            date_end = date_start

        if date_start > date_end:
            raise RequestException(
                f"start date {D.to_string(date_start)} must be earlier than end date {D.to_string(date_end)}")

        self.date_range_pairs = self.cache.determine_uncached_dates(date_start, date_end)
        # self.date_range_pairs.sort()

    @property
    def requires_query(self):
        return bool(self.date_range_pairs)

    def query(self, query, date_col):
        if not self.requires_query:
            return self.backend.query()

        if query.find(self.condition_placeholder) == -1:
            raise QueryGenerationException("Incorrect query configuration: date condition filter cannot be inserted")

        if query.find(self.table_name_prefix) == -1:
            raise QueryGenerationException("Incorrect query configuration: table names cannot be inserted")

        query = query.replace(self.condition_placeholder,
                              self.gen_sql_date_range_condition(date_col, self.date_range_pairs))

        # date range pairs should be mutually exclusive
        tables = self.get_table_groups(query, self.date_range_pairs[0][0], self.date_range_pairs[-1][1])

        if tables:
            for group_name, table_list in tables.items():
                query = query.replace(self.table_name.format(group_name),
                                      self.tables.gen_sql_query_tables(group_name, table_list))

            return self.backend.query(query)
        else:
            # this query won't return anything due to no relevant table. Execute empty query
            return self.backend.query()

    @staticmethod
    def gen_sql_date_range_condition(date_col, date_range_pairs):
        conditions = list()

        for date_start, date_end in date_range_pairs:
            start_date_str = D.to_string(date_start)
            end_date_str = D.to_string(date_end)

            if date_start == date_end:
                conditions.append(f"(date({date_col})='{start_date_str}')")
            else:
                conditions.append(f"(date({date_col})>='{start_date_str}' AND date({date_col})<='{end_date_str}')")

        return f"({' OR '.join(conditions)})"

    def get_table_groups(self, query, date_start, date_end):
        matched_table_names = re.findall(self.table_name_regex.format(r"\S+"), query)

        relevant_tables = dict()

        for table_name in matched_table_names:
            if table_name in relevant_tables:
                continue

            group_name = re.match(self.table_name_regex.format(r"(\S+)"), table_name).group(1)
            relevant_group_tables = self.tables.get_tables(group_name, date_start, date_end)

            if not relevant_group_tables:
                return dict()

            relevant_tables[group_name] = [table.sql_name for table in relevant_group_tables]

        return relevant_tables
