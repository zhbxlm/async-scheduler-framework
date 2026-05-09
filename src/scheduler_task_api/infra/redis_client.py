"""Redis client factory — package-owned implementation."""
from __future__ import annotations
import os
import logging
from typing import Any

logger = logging.getLogger(__name__)


async def create_redis_client(
    url: str | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 6379,
    password: str = "",
    db: int = 0,
    decode_responses: bool = True,
    max_connections: int = 50,
    use_ha: bool = True,
) -> Any:
    url = url or os.getenv("REDIS_URL", "")
    if not use_ha:
        import redis.asyncio as aioredis
        if url:
            return aioredis.from_url(url, decode_responses=decode_responses, max_connections=max_connections, retry_on_timeout=True, health_check_interval=30)
        return aioredis.Redis(host=host, port=port, password=password or None, db=db, decode_responses=decode_responses, max_connections=max_connections, retry_on_timeout=True, health_check_interval=30, socket_keepalive=True, socket_connect_timeout=5)
    from scheduler_task_api.common.redis_ha import RedisHA  # bundled: src/common/redis_ha.py
    sentinel_urls = os.getenv("REDIS_SENTINEL_URLS", "")
    master_name = os.getenv("REDIS_SENTINEL_MASTER", "mymaster")
    if sentinel_urls:
        sentinel_list = [s.strip() for s in sentinel_urls.split(",") if s.strip()]
        redis = RedisHA(sentinel_urls=sentinel_list, master_name=master_name, max_retries=3, circuit_breaker_threshold=5, circuit_breaker_timeout=30, degradation_mode=True)
    elif url:
        redis = RedisHA(redis_url=url, max_retries=3, circuit_breaker_threshold=5, circuit_breaker_timeout=30, degradation_mode=True)
    else:
        redis_url = f"redis://{host}:{port}/{db}"
        if password:
            redis_url = f"redis://:{password}@{host}:{port}/{db}"
        redis = RedisHA(redis_url=redis_url, max_retries=3, circuit_breaker_threshold=5, circuit_breaker_timeout=30, degradation_mode=True)
    try:
        await redis.start()
    except Exception as e:
        logger.error("RedisHA connection failed: %s", e)
        if not redis._is_connected and not redis.degradation_mode:
            raise
    return redis


async def close_redis_client(client) -> None:
    if hasattr(client, "close"):
        await client.close()
    elif hasattr(client, "aclose"):
        await client.aclose()
