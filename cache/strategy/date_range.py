from cache.backend.redis import RedisCache
from query.query_commons import QueryResponse
import pandas as pd


class DateRangeCache:

    host = "localhost"
    port = 6379
    password = None

    def __init__(self):
        self.c = RedisCache(host=self.host, port=self.port, password=self.password)
        self.curr_query = None
        self.cached_content = None

    def set_curr_query(self, query):
        self.curr_query = str(hash(query))

    def determine_uncached_dates(self, date_start, date_end):
        assert self.curr_query

        if date_start == date_end:
            self.cached_content = self.c.get(self.curr_query+str(date_start))

            if self.cached_content:
                return list()
            else:
                return [(date_start, date_start)]
        else:



    def merge_content(self, new_content, date_col):


