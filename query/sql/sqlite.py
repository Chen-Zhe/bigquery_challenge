import sqlite3
import pandas as pd
from query.query_commons import QueryResponse
import logging

logger = logging.getLogger(__name__)


class Sqlite3Backend:
    """
    Python SQLite 3 Backend
    """
    datetime_type = False

    def __init__(self, db_path="sample_data/nyc_taxi_sample_data.db"):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    def query(self, sql_string, limit=10000):
        query_job = self.cursor.execute(f"{sql_string} LIMIT {limit+1}")
        query_result = [row for row in query_job]
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
