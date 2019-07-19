from google.cloud.bigquery import Client
from query.query_commons import QueryResponse
from query.sql.tables.google_bigquery import tables
import logging

logger = logging.getLogger(__name__)


class BigQueryBackend:
    """
    Google BigQuery Backend
    """
    tables = tables

    def __init__(self, service_account_cred="../gojek-challenge-180b1791e462.json"):
        self.client = Client.from_service_account_json(service_account_cred)

    def query(self, sql_string="", limit=100000):
        if not sql_string:
            return QueryResponse(None, exceed_limit=False, is_empty=True)

        query_job = self.client.query(f"{sql_string} LIMIT {limit+1}")
        query_result = query_job.result()
        total_rows = query_result.total_rows
        if total_rows == 0:
            return QueryResponse(None, total_rows > limit, True)
        else:
            exceed_limit = total_rows > limit
            if exceed_limit:
                logger.warning(f"Query limit of {limit} is reached in query: {sql_string}")

            return QueryResponse(query_result.to_dataframe(), exceed_limit, False)
