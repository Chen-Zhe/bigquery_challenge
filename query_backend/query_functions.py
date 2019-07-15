from query_backend.sql.sqlite import Sqlite3Backend

def test():
    backend = Sqlite3Backend()
    query = (
        "SELECT date(dropoff_datetime) AS date, count(*) AS total_trips FROM `tlc_yellow_trips_2016` "
        "WHERE date(dropoff_datetime) <=  '2016-01-06' AND date(dropoff_datetime) >=  '2016-01-01'"
        "GROUP BY date(dropoff_datetime)")
    result = backend.query(query).response
    return [{key: value for (key, value) in zip(result.columns, row)}
            for row in result.values]

