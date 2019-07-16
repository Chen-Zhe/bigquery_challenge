from query.date_filter_frontend import DateFilterFrontend
from errors import *
from query.query_commons import HttpResponse
import json

backend = "sqlite"


@handle_exceptions
def total_trips_over_date_range(start_date, end_date):
    frontend = DateFilterFrontend(backend)

    query = (
        "SELECT date(dropoff_datetime) AS date, count(*) AS total_trips FROM `tlc_yellow_trips_2016` "
        f"WHERE {frontend.condition_placeholder} "
        "GROUP BY date(dropoff_datetime)")
    result = frontend.date_range_query(query, "dropoff_datetime", start_date, end_date)

    if result.empty_response:
        raise RequestException("Empty Result")
    else:
        json_format = [{key: value for (key, value) in zip(result.response.columns, row)}
                for row in result.response.values]
        response = HttpResponse(json.dumps(json_format), HttpResponse.ContentType.JSON, HttpResponse.Status.OK)

    return response

