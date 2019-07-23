from cache.backend.redis import RedisCache
from datetime import timedelta
import heapq
from query.types.date.utils import DateFormat as D
from io import BytesIO
import pyarrow.parquet as pq
import pandas as pd


def inclusive_date_range(date_start, date_end):
    for n in range((date_end - date_start).days + 1):
        yield date_start + timedelta(days=n)


class DateRangeCache:

    host = "localhost"
    port = 6379
    password = None
    max_intervals = 3

    def __init__(self):
        self.c = RedisCache(host=self.host, port=self.port, password=self.password)
        self.curr_query = None
        self.dates = None
        self.cached_content = None
        self.valid_cache = False

    def set_curr_query(self, query):
        self.curr_query = query

    def gen_cache_key(self, date):
        return self.curr_query + str(date)

    def determine_uncached_dates(self, date_start, date_end):
        if not self.curr_query:
            # curr_query is the key to access cache. Without the key, cache is unusable
            self.valid_cache = False
            return [(date_start, date_end)]

        self.dates = [date for date in inclusive_date_range(date_start, date_end)]
        self.cached_content = self.c.get_multi([self.gen_cache_key(date) for date in self.dates])

        cached_intervals = list()

        interval_start = None
        for i in range(len(self.dates)):
            if self.cached_content[i] is not None:
                if interval_start is None:
                    interval_start = (i, self.dates[i])
            else:
                if interval_start is not None:
                    idx, date = interval_start
                    heapq.heappush(cached_intervals, (idx - i + 1, date, self.dates[i-1]))
                    interval_start = None

        if interval_start is not None:
            idx, date = interval_start
            heapq.heappush(cached_intervals, (idx - len(self.dates), date, self.dates[-1]))

        if cached_intervals:
            longest_intervals = list()

            # for i in range(self.max_intervals - 1):
            #     _, start, end = heapq.heappop(cached_intervals)
            for _, start, end in cached_intervals:
                longest_intervals.append((start, end))

            longest_intervals.sort()
            uncached_intervals = list()
            one_day = timedelta(days=1)

            if date_start != longest_intervals[0][0]:
                uncached_intervals.append((date_start, longest_intervals[0][0] - one_day))
            if date_end != longest_intervals[-1][1]:
                uncached_intervals.append((longest_intervals[-1][1] + one_day, date_end))

            for i in range(0, len(longest_intervals)-1):
                uncached_intervals.append((longest_intervals[i][1] + one_day, longest_intervals[i+1][0] - one_day))

            self.valid_cache = True
            return uncached_intervals

        else:
            self.valid_cache = False
            return [(date_start, date_end)]

    def is_date_cached(self, date):
        self.cached_content = self.c.get(self.gen_cache_key(date))
        self.dates = date

        if self.cached_content is not None:
            self.valid_cache = True
            return True
        else:
            self.valid_cache = False
            return False

    def merge_multi_day_df(self, df, date_col):
        valid_dfs = list()
        uncached_dates = set()

        for date, cache in zip(self.dates, self.cached_content):
            if cache is None:
                uncached_dates.add(date)
            elif len(cache) != 0:
                valid_dfs.append(pq.read_table(BytesIO(cache)).to_pandas())

        if df is not None:
            valid_dfs.append(df)

            for date_str, group in df.groupby(date_col):
                date = D.validate_date_string(date_str)
                self.df_to_cache(date, group)
                uncached_dates.remove(date)

        # these uncached dates doesn't have data stored
        for date in uncached_dates:
            self.df_to_cache(date, None)

        return pd.concat(valid_dfs)

    def merge_single_day_df(self, df):
        if self.cached_content is not None:
            # the date is cached. return cached result
            if len(self.cached_content) == 0:
                return None
            else:
                return pq.read_table(BytesIO(self.cached_content)).to_pandas()

        else:
            if df is None:
                self.df_to_cache(self.dates, None)
            else:
                self.df_to_cache(self.dates, df)
            return df

    def df_to_cache(self, date, df):
        if df is None:
            self.c.set(self.gen_cache_key(date), "")
        else:
            buffer = BytesIO()
            df.to_parquet(buffer)
            self.c.set(self.gen_cache_key(date), buffer.getvalue())
