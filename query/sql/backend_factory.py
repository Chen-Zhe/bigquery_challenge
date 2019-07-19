from query.sql.google_bigquery import BigQueryBackend
from query.sql.sqlite import Sqlite3Backend
from errors import ServerException
from enum import Enum, auto


class SqlBackend(Enum):
    BIG_QUERY = auto()
    SQLITE = auto()


def get_sql_backend(name, *args):
    if name == SqlBackend.BIG_QUERY:
        return BigQueryBackend(*args)
    elif name == SqlBackend.SQLITE:
        return Sqlite3Backend(*args)
    else:
        raise ServerException(f"Unknown backend configured: '{name}'")
