from query.date_filter_frontend import DateFilterFrontend

backend = "sqlite"
frontend = DateFilterFrontend(backend)


def total_trips_over_date_range(start_date, end_date):
    # store main query
    # call corresponding
    query = (
        "SELECT date(dropoff_datetime) AS date, count(*) AS total_trips FROM `tlc_yellow_trips_2016` "
        f"WHERE {frontend.condition_placeholder}"
        "GROUP BY date(dropoff_datetime)")
    result = frontend.date_range_query(query, "dropoff_datetime", start_date, end_date).response
    return [{key: value for (key, value) in zip(result.columns, row)}
            for row in result.values]

