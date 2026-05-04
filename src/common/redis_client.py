"""Redis client factory — aligned with docs/deepwiki-reference/队列管理.md"""
from __future__ import annotations
import os
import redis.asyncio as aioredis


def create_redis_client(
    url: str | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 6379,
    password: str = "",
    db: int = 0,
    decode_responses: bool = True,
    max_connections: int = 50,
) -> aioredis.Redis:
    url = url or os.getenv("REDIS_URL", "")
    if url:
        return aioredis.from_url(url, decode_responses=decode_responses, max_connections=max_connections)
    return aioredis.Redis(
        host=host,
        port=port,
        password=password or None,
        db=db,
        decode_responses=decode_responses,
        max_connections=max_connections,
    )
