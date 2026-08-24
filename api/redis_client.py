"""api/redis_client.py"""
import os
import redis.asyncio as aioredis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

_pool: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _pool
    if _pool is None:
        _pool = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    return _pool


async def get_redis_raw() -> aioredis.Redis:
    return aioredis.from_url(REDIS_URL, decode_responses=False)


async def close_redis():
    global _pool
    if _pool:
        await _pool.aclose()
        _pool = None
