from query.sql.google_bigquery import BigQueryBackend
from query.sql.sqlite import Sqlite3Backend
from errors import ServerException


def get_sql_backend(name, *args):
    if name == "bigquery":
        return BigQueryBackend(*args)
    elif name == "sqlite":
        return Sqlite3Backend(*args)
    else:
        raise ServerException(f"Unknown backend configured: '{name}'")