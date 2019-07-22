from redis import Redis


class RedisCache:
    """
    Redis Cache interface
    It is designed as an optional component, such that losing connecting to Redis would not result in server error
    """
    def __init__(self, *args, **kwargs):
        self.r = Redis(*args, **kwargs)

        try:
            self.alive = self.r.ping()
        except:
            # ping would return True if connection is successful
            # all other cases would be treated as the cache is not alive
            self.alive = False

    def get(self, key):
        if not self.alive:
            return None

        return self.r.get(key)

    def get_multi(self, keys):
        if not self.alive:
            return list()

        return self.r.get(keys)
