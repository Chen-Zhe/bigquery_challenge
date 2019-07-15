from google.cloud.bigquery import Client
from query_backend.query_commons import QueryResponse


class BigQueryBackend:
    """
    Google BigQuery Backend
    """
    datetime_type = True

    def __init__(self, service_account_cred="../gojek-challenge-180b1791e462.json"):
        self.client = Client.from_service_account_json(service_account_cred)

    def query(self, sql_string, limit=10000):
        query_job = self.client.query(f"{sql_string} LIMIT {limit+1}")
        query_result = query_job.result()
        total_rows = query_result.total_rows
        if total_rows == 0:
            return QueryResponse(None, total_rows > limit, True)
        else:
            return QueryResponse(query_result.to_dataframe(), total_rows > limit, False)
