from query.types.date.date_filter_query import SqlDateFilter
from errors import *
import numpy as np
import pandas as pd
from query.query_commons import df2json_list, json2http_ok
from query.sql.backend_factory import SqlBackend
from cache.strategy.date_range import DateRangeCache

backend = SqlBackend.SQLITE


def add(s1, s2):
    return np.nan_to_num(s1) + np.nan_to_num(s2)


def whoami():
    import sys
    return sys._getframe(1).f_code.co_name


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
    f = SqlDateFilter(backend)

    date_col = "pickup_datetime"
    result_date = "date"

    result_dfs = list()

    cache = DateRangeCache()
    cache.set_curr_query(whoami())

    for table_group in ["tlc_green_trips", "tlc_yellow_trips"]:
        query = (
            f"SELECT date({date_col}) AS {result_date}, COUNT({date_col}) AS total_trips FROM {f.table_name.format(table_group)} "
            f"WHERE {f.condition_placeholder} AND {date_col} IS NOT NULL "
            f"GROUP BY date({date_col})")
        result = f.date_range_query(query, date_col, start_date, end_date)

        if not result.is_empty:
            result_dfs.append(result.response)

    if not result_dfs:
        raise RequestException("Empty Result")

    merged_result = join_dataframes(result_dfs, result_date)

    return json2http_ok(df2json_list(merged_result))


@handle_exceptions
def average_fare_heatmap_of_date(date):
    f = SqlDateFilter(backend)

    from s2sphere import Cell, LatLng

    def calc_s2id(lat, lng, level):
        s2id = Cell.from_lat_lng(LatLng(lat, lng)).id().parent(level).id()
        return f"{s2id:016x}"

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

        if not result.is_empty:
            result_dfs.append(result.response)

    if not result_dfs:
        raise RequestException("Empty Result")

    all_trips = pd.concat(result_dfs)
    all_trips[s2id] = all_trips.apply(lambda x: calc_s2id(x[lat], x[lng], 16)[:9], axis=1)

    result = all_trips.groupby(s2id, as_index=False).agg({fare_amount: "mean"})
    return json2http_ok(df2json_list(result))
    

@handle_exceptions
def average_speed_of_date(date):
    f = SqlDateFilter(backend)

    result_dfs = list()
    date_col = "pickup_datetime"
    trip_count = "trip_count"
    avg_speed = "average_speed"

    timediff_part = "SECOND"
    if backend == SqlBackend.SQLITE:
        timediff_part = f"'{timediff_part}'"

    for table_group in ["tlc_green_trips", "tlc_yellow_trips"]:
        query = (
            f"SELECT AVG(trip_distance / DATETIME_DIFF(dropoff_datetime, pickup_datetime, {timediff_part}) * 3600) AS {avg_speed}, "
            f"COUNT({date_col}) AS {trip_count} "
            f"FROM {f.table_name.format(table_group)} "
            f"WHERE {f.condition_placeholder} "
            f"AND dropoff_datetime IS NOT NULL AND pickup_datetime IS NOT NULL AND trip_distance IS NOT NULL "
            f"GROUP BY date({date_col})")
        result = f.date_query(query, date_col, date)

        if not result.is_empty:
            result_dfs.append(result.response)

    if not result_dfs:
        raise RequestException("Empty Result")

    all_avg_speed = pd.concat(result_dfs)
    response = pd.DataFrame(data={avg_speed: [(all_avg_speed[trip_count] / all_avg_speed[trip_count].sum() * all_avg_speed[avg_speed]).sum()]})

    return json2http_ok(df2json_list(response))
