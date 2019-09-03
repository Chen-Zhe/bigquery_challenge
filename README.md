# BigQuery Challenge

## Setup Process
This setup process assumes an installation on Ubuntu 

1. [Install](https://www.anaconda.com/distribution/) Anaconda Python
1. Install additional python packages

    ```
    pip install s2sphere google-cloud-bigquery
    conda install redis-py pyarrow
    ```
    
1. Install and Configure Redis (Optional)

    1. `apt-get install redis-server`
    1. [Configure](https://redis.io/topics/lru-cache) the Redis instance to be a LRU cache with `maxmemory-policy` set to `allkeys-lru`
    2. start Redis server `service redis-server start`

1. Run `server.py` for a development server or connect it to a WSGI server.

## Feedbacks
1. have integration test, unit test
2. OODP
3. logging - could be more careful
4. behavior is correct, error-handling is correct
5. analyze data in details

concerns

6. a little bit hard for it to run on system. no documentation on the credentials and no build scripts for docker image.
7. init.py usage is not up to standard
8. date filter to have optional to put in cache
9. unit test: could have better coverage
