from query.date_filter_frontend import SqlDateFilter, DataTable, SqlTableCollection
from errors import *
import numpy as np
import pandas as pd
import s2sphere
from query.query_commons import HttpResponse
import json

backend = "sqlite"

tables = SqlTableCollection(["tlc_green_trips", "tlc_yellow_trips"])
if backend == "sqlite":
    tables.register_table("tlc_green_trips", DataTable("tlc_green_trips_2016", "2016-01-01", "2016-12-31"))
    tables.register_table("tlc_yellow_trips", DataTable("tlc_yellow_trips_2016", "2016-01-01", "2016-12-31"))
else:
    tables.register_table("tlc_green_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2014",
                                                       "2014-01-01", "2014-12-31"))
    tables.register_table("tlc_green_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2015",
                                                       "2015-01-01", "2015-12-31"))
    tables.register_table("tlc_green_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2016",
                                                       "2016-01-01", "2016-12-31"))
    tables.register_table("tlc_green_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2017",
                                                       "2017-01-01", "2017-12-31"))
    tables.register_table("tlc_yellow_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2015",
                                                        "2015-01-01", "2015-12-31"))
    tables.register_table("tlc_yellow_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2016",
                                                        "2016-01-01", "2016-12-31"))
    tables.register_table("tlc_yellow_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2017",
                                                        "2017-01-01", "2017-12-31"))


def add(s1, s2):
    return np.nan_to_num(s1) + np.nan_to_num(s2)


def join_dataframes(df_list, key_col, merge=add):
    if not df_list:
        raise ServerException("Attempting to merge an empty list of dataframe")

    result = df_list[0]

    value_columns = result.drop([key_col], axis=1).columns
    lsuffix = "_l"
    rsuffix = "_r"

    for df in df_list[1:]:
        temp = result.join(df.set_index(key_col), on=key_col, lsuffix=lsuffix, rsuffix=rsuffix, how="outer")
        for col in value_columns:
            temp[col] = merge(temp[col + lsuffix], temp[col + rsuffix])
            temp = temp.drop([col + lsuffix, col + rsuffix], axis=1)
        result = temp

    return result


@handle_exceptions
def total_trips_over_date_range(start_date, end_date):
    f = SqlDateFilter(backend, tables)

    date_col = "pickup_datetime"
    result_dfs = list()

    for table_group in ["tlc_green_trips", "tlc_yellow_trips"]:
        query = (
            f"SELECT date({date_col}) AS date, COUNT(*) AS total_trips FROM {f.table_name.format(table_group)} "
            f"WHERE {f.condition_placeholder} "
            f"GROUP BY date({date_col})")
        result = f.date_range_query(query, date_col, start_date, end_date)

        if not result.empty_response:
            result_dfs.append(result.response)

    if not result_dfs:
        raise RequestException("Empty Result")

    merged_result = join_dataframes(result_dfs, "date")

    json_format = {"data": [{key: value for (key, value) in zip(merged_result.columns, row)}
                   for row in merged_result.values], "status": "OK"}
    response = HttpResponse(json.dumps(json_format), HttpResponse.ContentType.JSON, HttpResponse.Status.OK)

    return response


@handle_exceptions
def average_fare_heatmap_for_date(date):
    f = SqlDateFilter(backend, tables)

    date_col = "pickup_datetime"
    result_dfs = list()

    for table_group in ["tlc_green_trips", "tlc_yellow_trips"]:
        query = (
            f"SELECT pickup_latitude, pickup_longitude, fare_amount FROM {f.table_name.format(table_group)} "
            f"WHERE {f.condition_placeholder} "
            f"WHERE pickup_latitude IS NOT NULL AND  pickup_longitude IS NOT NULL")
        result = f.date_query(query, date_col, date)

        if not result.empty_response:
            result_dfs.append(result.response)

    if not result_dfs:
        raise RequestException("Empty Result")

    all_trips = pd.concat(result_dfs)
    

