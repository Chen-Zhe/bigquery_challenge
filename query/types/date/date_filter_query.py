import re
from datetime import datetime

from cache.strategy.date_range import DateRangeCache
from http.errors import *
from query.sql.backend_factory import get_sql_backend
from query.types.date.utils import DateFormat as D


class SqlDateFilter:
    """Complete Date-related SQL queries with support for caching"""

    condition_placeholder = "#DateCondition"
    table_name_prefix = "#TableGroup"
    table_name = table_name_prefix + "({})"
    table_name_regex = table_name_prefix + r"\({}\)"

    def __init__(self, backend, query_name):
        """
        Constructor
        :param backend: Selected backend (as a Enum member)
        :param query_name: Unique identifier (string) of the query as the cache key
        """
        self.backend = get_sql_backend(backend)
        self.tables = self.backend.tables
        self.cache = DateRangeCache(str(backend) + query_name)
        self.date_range_pairs = None

    def set_date_range(self, start_date, end_date):
        """
        Set querying date range for subsequent queries
        :param start_date: date string in valid format or None
        :param end_date: date string in valid format or None
        :return: nothing
        """
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
        """
        Set querying date for subsequent queries and check if result of the date is cached
        :param date: date string in valid format or None
        :return: nothing
        """
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
        """
        helper function for set_date_range to validate date range and query cache for uncached date ranges
        :param date_start:
        :param date_end:
        :return:
        """
        if date_end is None:
            date_end = date_start

        if date_start > date_end:
            raise RequestException(
                f"start date {D.date_to_string(date_start)} must be earlier than end date {D.date_to_string(date_end)}")

        self.date_range_pairs = self.cache.determine_uncached_dates(date_start, date_end)
        # self.date_range_pairs.sort()

    @property
    def requires_query(self):
        """data_range_pairs would be set to empty if all results are cached"""
        return bool(self.date_range_pairs)

    def query(self, query, date_col):
        """
        Automatically complete a SQL query in terms of date filter condition and tables
        :param query: incomplete SQL query with placeholders for date filter condition and tables
        :param date_col: date column to use in filtering in the SQL table
        :return: QueryResponse
        """
        if not self.requires_query:
            return self.backend.query()

        if query.find(self.condition_placeholder) == -1:
            raise QueryGenerationException("Incorrect query configuration: date condition filter cannot be inserted")

        if query.find(self.table_name_prefix) == -1:
            raise QueryGenerationException("Incorrect query configuration: table names cannot be inserted")

        # generate and replace placeholder for date filter conditions
        query = query.replace(self.condition_placeholder,
                              self.gen_sql_date_range_condition(date_col, self.date_range_pairs))

        # date range pairs should be mutually exclusive
        # get and replace placeholder for relevant tables
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
        """
        Generate SQL date range conditions given a list of date range pairs
        :param date_col: date column to use in filtering
        :param date_range_pairs: list of date range pairs (both ends are inclusive)
        :return: SQL filter condition string
        """
        conditions = list()

        for date_start, date_end in date_range_pairs:
            start_date_str = D.date_to_string(date_start)
            end_date_str = D.date_to_string(date_end)

            if date_start == date_end:
                conditions.append(f"(date({date_col})='{start_date_str}')")
            else:
                conditions.append(f"(date({date_col})>='{start_date_str}' AND date({date_col})<='{end_date_str}')")

        return f"({' OR '.join(conditions)})"

    def get_table_groups(self, query, date_start, date_end):
        """
        Find all table group placeholders within a query and find the relevant tables
        :param query: SQL query string
        :param date_start: starting date
        :param date_end: end date
        :return: dict of table group name with list of tables
        """
        matched_table_names = re.findall(self.table_name_regex.format(r"\S+"), query)

        relevant_tables = dict()

        for table_name in matched_table_names:
            if table_name in relevant_tables:
                continue

            group_name = re.match(self.table_name_regex.format(r"(\S+)"), table_name).group(1)
            relevant_group_tables = self.tables.get_tables(group_name, date_start, date_end)

            if not relevant_group_tables:
                return dict()

            relevant_tables[group_name] = relevant_group_tables

        return relevant_tables
