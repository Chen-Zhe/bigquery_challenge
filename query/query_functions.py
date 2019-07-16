from query.date_filter_frontend import DateFilterFrontend
from errors import handle_exceptions
from query.query_commons import HttpResponse
import json

backend = "sqlite"
frontend = DateFilterFrontend(backend)


@handle_exceptions
def total_trips_over_date_range(start_date, end_date):
    query = (
        "SELECT date(dropoff_datetime) AS date, count(*) AS total_trips FROM `tlc_yellow_trips_2016` "
        f"WHERE {frontend.condition_placeholder}"
        "GROUP BY date(dropoff_datetime)")
    result = frontend.date_range_query(query, "dropoff_datetime", start_date, end_date).response

    json_format = [{key: value for (key, value) in zip(result.columns, row)}
            for row in result.values]

    response = HttpResponse(json.dumps(json_format), HttpResponse.ContentType.JSON, HttpResponse.Status.OK)

    return response

