# BigQuery Challenge

## Setup Process
This setup process assumes an installation on Ubuntu 

1. [Install](https://www.anaconda.com/distribution/) Anaconda Python
1. Install additional python packages

    ```
    pip install s2sphere
    conda install redis-py
    ```
    
1. Install and Configure Redis (Optional)

    1. `apt-get install redis-server`
    1. [Configure](https://redis.io/topics/lru-cache) the Redis instance on port 6380 to be a LRU cache with `maxmemory-policy` set to `allkeys-lru`
    2. start Redis server `service redis-server start`

1. Run `server.py` for a development server or connect it to a WSGI server.