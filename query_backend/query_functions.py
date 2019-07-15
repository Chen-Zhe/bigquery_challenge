from query_backend.backend_factory import get_sql_backend

backend = "sqlite"

def total_trips_over_date_range(start_date, end_date):
    sql_backend = get_sql_backend(backend)
    query = (
        "SELECT date(dropoff_datetime) AS date, count(*) AS total_trips FROM `tlc_yellow_trips_2016` "
        "WHERE date(dropoff_datetime) <=  '2016-01-06' AND date(dropoff_datetime) >=  '2016-01-01'"
        "GROUP BY date(dropoff_datetime)")
    result = sql_backend.query(query).response
    return [{key: value for (key, value) in zip(result.columns, row)}
            for row in result.values]

