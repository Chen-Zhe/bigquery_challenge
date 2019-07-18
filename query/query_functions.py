from query.date_filter_frontend import SqlDateFilter, DataTable, SqlTableCollection
from errors import *
import numpy as np
import pandas as pd
from query.query_commons import df2json_dict, json2http

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
    tables.register_table("tlc_yellow_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2015",
                                                        "2015-01-01", "2015-12-31"))
    tables.register_table("tlc_yellow_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2016",
                                                        "2016-01-01", "2016-12-31"))
    tables.register_table("tlc_yellow_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2017",
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
            f"SELECT date({date_col}) AS date, COUNT({date_col}) AS total_trips FROM {f.table_name.format(table_group)} "
            f"WHERE {f.condition_placeholder} AND {date_col} IS NOT NULL "
            f"GROUP BY date({date_col})")
        result = f.date_range_query(query, date_col, start_date, end_date)

        if not result.empty_response:
            result_dfs.append(result.response)

    if not result_dfs:
        raise RequestException("Empty Result")

    merged_result = join_dataframes(result_dfs, "date")

    return json2http(df2json_dict(merged_result))


@handle_exceptions
def average_fare_heatmap_of_date(date):
    from s2sphere import Cell, LatLng

    def calc_s2id(lat, lng, level):
        s2id = Cell.from_lat_lng(LatLng(lat, lng)).id().parent(level).id()
        return f"{s2id:016x}"

    f = SqlDateFilter(backend, tables)

    lat = "lat"
    lng = "lng"
    fare_amount = "fare_amount"
    date_col = "pickup_datetime"
    s2id = "s2id"
    result_dfs = list()

    for table_group in ["tlc_green_trips", "tlc_yellow_trips"]:
        query = (
            f"SELECT pickup_latitude AS {lat}, pickup_longitude AS {lng}, {fare_amount} FROM {f.table_name.format(table_group)} "
            f"WHERE {f.condition_placeholder} "
            f"AND pickup_latitude IS NOT NULL AND pickup_longitude IS NOT NULL AND {fare_amount} IS NOT NULL")
        result = f.date_query(query, date_col, date)

        if not result.empty_response:
            result_dfs.append(result.response)

    if not result_dfs:
        raise RequestException("Empty Result")

    all_trips = pd.concat(result_dfs)
    all_trips[s2id] = all_trips.apply(lambda x: calc_s2id(x[lat], x[lng], 16)[:9], axis=1)

    result = all_trips.groupby(s2id, as_index=False).agg({fare_amount: "mean"})
    return json2http(df2json_dict(result))
    

