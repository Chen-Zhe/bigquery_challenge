# This is the configuration file for all components


class DateRangeCacheConfig:
    host = "localhost"
    port = 6379
    password = None
    socket_connect_timeout = 1
    socket_timeout = 1


class SqlBackendConfig:
    from query.sql.backend_factory import SqlBackend
    backend = SqlBackend.BIG_QUERY


class DataFormatConfig:
    supported_date_format = "%Y-%m-%d"


class DevServerConfig:
    host = "localhost"
    port = 8080
    debug = True


class BigQueryConfig:
    default_credential_file = "../gojek-challenge-180b1791e462.json"
    default_query_limit = 100000


class SqliteConfig:
    database_file = "sample_data/nyc_taxi_sample_data.db"
    default_query_limit = 10000
