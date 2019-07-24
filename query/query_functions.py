import numpy as np
import pandas as pd

from conf import SqlBackendConfig
from errors import *
from query.query_commons import df2json_list, json2http
from query.sql.backend_factory import SqlBackend
from query.types.date.date_filter_query import SqlDateFilter

backend = SqlBackendConfig.backend


def add_func(s1, s2):
    return np.nan_to_num(s1) + np.nan_to_num(s2)


def whoami():
    import sys
    return sys._getframe(1).f_code.co_name


def join_dataframes(df_list, key_col, merge):
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
    from query.table_schema import YellowGreenCommonFields as CF, ResultCommonFields as RCF

    class TF:
        total_trips = "total_trips"

    f = SqlDateFilter(backend, whoami())
    f.set_date_range(start_date, end_date)

    date_col = CF.pickup_datetime
    result_dfs = list()
    partial_result = False

    if f.requires_query:
        for table_group in ["tlc_green_trips", "tlc_yellow_trips"]:
            query = (
                f"SELECT date({date_col}) AS {RCF.date}, COUNT({date_col}) AS {TF.total_trips} "
                f"FROM {f.table_name.format(table_group)} "
                f"WHERE {f.condition_placeholder} AND {date_col} IS NOT NULL "
                f"GROUP BY date({date_col})")
            result = f.query(query, date_col)

            if not result.is_empty:
                result_dfs.append(result.response)

            partial_result = partial_result or result.exceed_limit

    if not result_dfs:
        query_result_df = None
    else:
        query_result_df = join_dataframes(result_dfs, RCF.date, add_func)

    merged_result = f.cache.merge_multi_day_df(query_result_df, RCF.date)

    if merged_result is None:
        raise RequestException("Empty Result")

    return json2http(df2json_list(merged_result), partial_result)


@handle_exceptions
def average_fare_heatmap_of_date(date):
    # TODO: Use C++ library with Python wrapper from s2geometry instead
    from s2sphere import Cell, LatLng
    from query.table_schema import YellowGreenCommonFields as CF

    class TF:
        lat = "lat"
        lng = "lng"
        s2id = "s2id"

    def calc_s2id(lat, lng, level):
        s2id = Cell.from_lat_lng(LatLng(lat, lng)).id().parent(level).id()
        return f"{s2id:016x}"

    f = SqlDateFilter(backend, whoami())
    f.set_date(date)

    date_col = CF.pickup_datetime

    result_dfs = list()
    partial_result = False

    if f.requires_query:
        for table_group in ["tlc_green_trips", "tlc_yellow_trips"]:
            query = (
                f"SELECT {CF.pickup_latitude} AS {TF.lat}, {CF.pickup_longitude} AS {TF.lng}, {CF.fare_amount} "
                f"FROM {f.table_name.format(table_group)} "
                f"WHERE {f.condition_placeholder} "
                f"AND {CF.pickup_latitude} IS NOT NULL AND {CF.pickup_longitude} IS NOT NULL "
                f"AND {CF.fare_amount} IS NOT NULL")
            result = f.query(query, date_col)

            if not result.is_empty:
                result_dfs.append(result.response)

            partial_result = partial_result or result.exceed_limit

    if not result_dfs:
        query_result_df = None
    else:
        all_trips = pd.concat(result_dfs)
        all_trips[TF.s2id] = all_trips.apply(lambda x: calc_s2id(x[TF.lat], x[TF.lng], 16)[:9], axis=1)
        query_result_df = all_trips.groupby(TF.s2id, as_index=False).agg({CF.fare_amount: "mean"})

    merged_result = f.cache.merge_single_day_df(query_result_df)
    if merged_result is None:
        raise RequestException("Empty Result")

    return json2http(df2json_list(merged_result), partial_result)


@handle_exceptions
def average_speed_of_date(date):
    from query.table_schema import YellowGreenCommonFields as CF

    class TF:
        trip_count = "trip_count"
        avg_speed = "average_speed"
        timediff_part = "SECOND"
        if backend == SqlBackend.SQLITE:
            timediff_part = f"'{timediff_part}'"

    f = SqlDateFilter(backend, whoami())
    f.set_date(date)

    partial_result = False
    result_dfs = list()
    date_col = CF.pickup_datetime

    if f.requires_query:
        for table_group in ["tlc_green_trips", "tlc_yellow_trips"]:
            query = (
                f"SELECT AVG({CF.trip_distance} / "
                f"DATETIME_DIFF({CF.dropoff_datetime}, {CF.pickup_datetime}, {TF.timediff_part}) * 3600) "
                f"AS {TF.avg_speed}, "
                f"COUNT({date_col}) AS {TF.trip_count} "
                f"FROM {f.table_name.format(table_group)} "
                f"WHERE {f.condition_placeholder} "
                f"AND {CF.dropoff_datetime} IS NOT NULL AND {CF.pickup_datetime} IS NOT NULL "
                f"AND {CF.trip_distance} IS NOT NULL AND {CF.dropoff_datetime} != {CF.pickup_datetime} "
                f"GROUP BY date({date_col})")
            result = f.query(query, date_col)

            if not result.is_empty:
                result_dfs.append(result.response)

            partial_result = partial_result or result.exceed_limit

    if not result_dfs:
        query_result_df = None
    else:
        all_avg_speed = pd.concat(result_dfs)
        query_result_df = pd.DataFrame(data={TF.avg_speed: [
            (all_avg_speed[TF.trip_count] / all_avg_speed[TF.trip_count].sum() * all_avg_speed[TF.avg_speed]).sum()]})

    merged_result = f.cache.merge_single_day_df(query_result_df)
    if merged_result is None:
        raise RequestException("Empty Result")

    return json2http(df2json_list(merged_result), partial_result)
