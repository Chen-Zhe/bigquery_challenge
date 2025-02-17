import logging
import sqlite3

import dateutil.parser
import pandas as pd

from conf import SqliteConfig
from query.query_commons import QueryResponse
from query.sql.tables.sqlite import tables

logger = logging.getLogger(__name__)


def _datetime_diff(datetime_end_str, datetime_start_str, part):
    """
    Calculate the difference between 2 datetime strings by defined accuracy (part)
    Compatibility function to allow interoperable query with Google Big Query
    :param datetime_end_str: datetime string
    :param datetime_start_str: datetime string
    :param part: accuracy. Currently supports DAY, SECOND, HOUR
    :return: int
    """
    try:
        date_end = dateutil.parser.parse(datetime_end_str)
        date_start = dateutil.parser.parse(datetime_start_str)

        diff = (date_end - date_start).total_seconds()

        if part == "DAY":
            return int(diff / 86400)
        elif part == "SECOND":
            return int(diff)
        elif part == "HOUR":
            return int(diff / 3600)

    except:
        return None

    return None


class Sqlite3Backend:
    """
    Python SQLite 3 Backend
    """
    tables = tables

    def __init__(self, db_path=SqliteConfig.database_file):
        """
        Constructor
        :param db_path: path to Sqlite Database file
        """
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

        # register compatibility function with Sqlite
        self.conn.create_function("DATETIME_DIFF", 3, _datetime_diff)

    def query(self, sql_string="", limit=SqliteConfig.default_query_limit):
        """
        Run SQL query in this backend
        :param sql_string: SQL string
        :param limit: return row limit
        :return: QueryResponse
        """
        if not sql_string:
            return QueryResponse(None, exceed_limit=False, is_empty=True)

        query_job = self.cursor.execute(f"{sql_string} LIMIT {limit + 1}")
        query_result = query_job.fetchall()

        total_rows = len(query_result)
        if total_rows == 0:
            return QueryResponse(None, total_rows > limit, True)
        else:
            columns = query_result[0].keys()
            data = [dict(row) for row in query_result]

            exceed_limit = total_rows > limit
            if exceed_limit:
                logger.warning(f"Query limit of {limit} is reached in query: {sql_string}")

        return QueryResponse(pd.DataFrame(data=data, columns=columns), total_rows > limit, False)
