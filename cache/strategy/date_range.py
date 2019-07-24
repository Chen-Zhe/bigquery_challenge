from datetime import timedelta

import pandas as pd

from cache.backend.redis import RedisCache
from cache.backend.serialize import DataFrameSerializer as DFS
from conf import DateRangeCacheConfig
from query.types.date.utils import DateFormat as D


def inclusive_date_range(date_start, date_end):
    """
    Generate an iterator of dates between start and end (include both ends)
    :param date_start: start date
    :param date_end: end date
    :return: iterator
    """
    for n in range((date_end - date_start).days + 1):
        yield date_start + timedelta(days=n)


class DateRangeCache:
    """
    Caching strategy for data of a certain date range or a single date
    """

    Conf = DateRangeCacheConfig

    def __init__(self, query_name):
        self.c = RedisCache(host=self.Conf.host, port=self.Conf.port, password=self.Conf.password)
        self.curr_query = query_name
        self.dates = None
        self.cached_content = None

    def gen_cache_key(self, date):
        return self.curr_query + str(date)

    def determine_uncached_dates(self, date_start, date_end):
        if not self.curr_query:
            # curr_query is the key to access cache. Without the key, cache is unusable
            return [(date_start, date_end)]

        self.dates = [date for date in inclusive_date_range(date_start, date_end)]
        self.cached_content = self.c.get_multi([self.gen_cache_key(date) for date in self.dates])

        cached_intervals = list()

        interval_start = None
        for i in range(len(self.cached_content)):
            if self.cached_content[i] is not None:
                if interval_start is None:
                    interval_start = (i, self.dates[i])
            else:
                if interval_start is not None:
                    idx, date = interval_start
                    cached_intervals.append((date, self.dates[i - 1]))
                    interval_start = None

        if interval_start is not None:
            idx, date = interval_start
            cached_intervals.append((date, self.dates[-1]))

        if cached_intervals:
            cached_intervals.sort()
            one_day = timedelta(days=1)

            uncached_intervals = list()

            if date_start != cached_intervals[0][0]:
                uncached_intervals.append((date_start, cached_intervals[0][0] - one_day))

            for i in range(0, len(cached_intervals) - 1):
                uncached_intervals.append((cached_intervals[i][1] + one_day, cached_intervals[i + 1][0] - one_day))

            if date_end != cached_intervals[-1][1]:
                uncached_intervals.append((cached_intervals[-1][1] + one_day, date_end))

            return uncached_intervals

        else:
            return [(date_start, date_end)]

    def is_date_cached(self, date):
        self.cached_content = self.c.get(self.gen_cache_key(date))
        self.dates = date

        if self.cached_content is not None:
            return True
        else:
            return False

    def merge_multi_day_df(self, df, date_col):
        valid_dfs = list()
        uncached_dates = set(self.dates)
        key_value_to_cache = dict()

        for date, cache in zip(self.dates, self.cached_content):
            if cache is not None:
                uncached_dates.remove(date)
                date_df = DFS.deserialize(cache)
                if date_df is not None:
                    valid_dfs.append(date_df)

        if df is not None:
            valid_dfs.append(df)

            for date, group in df.groupby(date_col):
                # assumes that date column is either a valid date string or date object
                if isinstance(date, str):
                    date = D.validate_date_string(date)

                k, v = self.df2key_value_pairs(date, group)
                key_value_to_cache[k] = v
                uncached_dates.remove(date)

        # these uncached dates doesn't have data stored, so a fill-in is required
        for date in uncached_dates:
            k, v = self.df2key_value_pairs(date, None)
            key_value_to_cache[k] = v

        self.c.set_multi(key_value_to_cache)

        if valid_dfs:
            # There are DataFrames to be merged
            return pd.concat(valid_dfs)
        else:
            # No DataFrame to be merged
            return None

    def merge_single_day_df(self, df):
        if self.cached_content is not None:
            # the date is cached. return cached result
            return DFS.deserialize(self.cached_content)
        else:
            # Write query result to cache
            k, v = self.df2key_value_pairs(self.dates, df)
            self.c.set(k, v)
            return df

    def df2key_value_pairs(self, date, df):
        # None case is handled by serializer
        return self.gen_cache_key(date), DFS.serialize(df)
