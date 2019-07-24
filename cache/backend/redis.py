import logging

from redis import Redis

logger = logging.getLogger(__name__)


class RedisCache:
    """
    Redis Cache interface
    It is designed as an optional component, such that losing connecting to Redis would not result in server error
    """

    def __init__(self, *args, **kwargs):
        """Initialize Redis connection with all parameters passed to Redis Python interface"""
        self.r = Redis(*args, **kwargs)

        try:
            self.alive = self.r.ping()
        except:
            logger.warning("Cache offline")
            # ping would return True if connection is successful
            # all other cases would be treated as if the cache is not alive
            self.alive = False

    def get(self, key):
        """
        Get key from cache
        :param key: cache key
        :return: cache value. None if cache is offline/key does not exist/encountered exception when retrieving result
        """
        if not self.alive:
            return None
        try:
            return self.r.get(key)
        except:
            logger.exception("Exception occurred when reading one key from cache")
            return None

    def get_multi(self, keys):
        """
        get multiple keys from cache at the same time
        :param keys: list of keys
        :return: list of values in the same length as keys. Empty list if cache is offline/encountered exception when retrieving result/no key is specified
        """
        if not self.alive or not keys:
            return list()
        try:
            return self.r.mget(keys)
        except:
            logger.exception("Exception occurred when reading multiple keys from cache")
            return list()

    def set(self, key, value):
        """
        Set one key with value in cache. Overwrites existing key
        :param key: key in cache
        :param value: value to be set
        :return: Nothing
        """
        if self.alive:
            try:
                self.r.set(key, value)
            except:
                logger.exception("Exception occurred when writing to cache")
                pass

    def set_multi(self, key_value_map):
        """
        Set multiple keys with values in cache. Overwrites existing key
        :param key_value_map: key-value map
        :return: Nothing
        """
        if self.alive and key_value_map:
            try:
                self.r.mset(key_value_map)
            except:
                logger.exception("Exception occurred when writing to cache")
                pass
