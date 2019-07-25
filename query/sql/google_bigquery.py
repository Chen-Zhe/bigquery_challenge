import logging

from google.cloud.bigquery import Client

from conf import BigQueryConfig
from query.query_commons import QueryResponse
from query.sql.tables.google_bigquery import tables
from query.table_schema import ResultCommonFields as RCF
from query.types.date.utils import DateFormat as D

logger = logging.getLogger(__name__)


class BigQueryBackend:
    """
    Google BigQuery Backend
    """
    tables = tables

    def __init__(self, service_account_cred=BigQueryConfig.default_credential_file):
        """
        Constructor
        :param service_account_cred: service account access key as a JSON file
        """
        self.client = Client.from_service_account_json(service_account_cred)

    def query(self, sql_string="", limit=BigQueryConfig.default_query_limit):
        """
        Run SQL query in this backend
        :param sql_string: SQL string
        :param limit: return row limit
        :return: QueryResponse
        """
        if not sql_string:
            return QueryResponse(None, exceed_limit=False, is_empty=True)

        # limit + 1 allows identification of whether the limit is exceeded in the query
        query_job = self.client.query(f"{sql_string} LIMIT {limit + 1}")
        query_result = query_job.result()
        total_rows = query_result.total_rows
        if total_rows == 0:
            return QueryResponse(None, total_rows > limit, True)
        else:
            exceed_limit = total_rows > limit
            if exceed_limit:
                logger.warning(f"Query limit of {limit} is reached in query: {sql_string}")

            df = query_result.to_dataframe()
            # Convert date object into string
            # This is because string can be serialized consistently but the objects cannot
            if RCF.date in df.columns:
                df[RCF.date] = df.apply(lambda x: D.date_to_string(x[RCF.date]), axis=1)

            return QueryResponse(df, exceed_limit, False)
