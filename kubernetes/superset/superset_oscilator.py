import os

SQLALCHEMY_DATABASE_URI = "postgresql://superset:asdf@superset-oscilator-postgres:5432/superset"

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": "redis",
    "CACHE_REDIS_PORT": 6379,
}

SUPERSET_WEBSERVER_TIMEOUT = 300
ENABLE_PROXY_FIX = True
ROW_LIMIT = 10000

SQLALCHEMY_TRACK_MODIFICATIONS = False

