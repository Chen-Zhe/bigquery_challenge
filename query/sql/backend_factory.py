from enum import Enum, auto

from errors import ServerException


class SqlBackend(Enum):
    BIG_QUERY = auto()
    SQLITE = auto()


def get_sql_backend(name, *args):
    if name == SqlBackend.BIG_QUERY:
        from query.sql.google_bigquery import BigQueryBackend
        return BigQueryBackend(*args)
    elif name == SqlBackend.SQLITE:
        from query.sql.sqlite import Sqlite3Backend
        return Sqlite3Backend(*args)
    else:
        raise ServerException(f"Unknown backend configured: '{name}'")
