"""Redis client factory — aligned with docs/deepwiki-reference/队列管理.md

Returns a RedisHA wrapper with retry, circuit breaker, and graceful degradation.
"""
from __future__ import annotations
import os
import logging
from typing import Any, Optional

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
    """Create a Redis client with optional HA features.
    
    Args:
        use_ha: If True, returns RedisHA wrapper with retry/circuit-breaker/degradation.
                If False, returns raw aioredis.Redis for compatibility.
    """
    url = url or os.getenv("REDIS_URL", "")
    
    if not use_ha:
        # Fallback to raw Redis client (for tests or when HA not needed)
        import redis.asyncio as aioredis
        if url:
            return aioredis.from_url(
                url,
                decode_responses=decode_responses,
                max_connections=max_connections,
                retry_on_timeout=True,
                health_check_interval=30,
            )
        return aioredis.Redis(
            host=host,
            port=port,
            password=password or None,
            db=db,
            decode_responses=decode_responses,
            max_connections=max_connections,
            retry_on_timeout=True,
            health_check_interval=30,
            socket_keepalive=True,
            socket_connect_timeout=5,
        )
    
    # Use RedisHA wrapper
    from src.common.redis_ha import RedisHA
    
    # Determine if we should use Sentinel
    sentinel_urls = os.getenv("REDIS_SENTINEL_URLS", "")
    master_name = os.getenv("REDIS_SENTINEL_MASTER", "mymaster")
    
    if sentinel_urls:
        # Sentinel mode
        sentinel_list = [s.strip() for s in sentinel_urls.split(",") if s.strip()]
        redis = RedisHA(
            sentinel_urls=sentinel_list,
            master_name=master_name,
            max_retries=3,
            circuit_breaker_threshold=5,
            circuit_breaker_timeout=30,
            degradation_mode=True,
        )
        logger.info("Creating RedisHA with Sentinel: %s (master=%s)", sentinel_list, master_name)
    elif url:
        # Standalone with URL
        redis = RedisHA(
            redis_url=url,
            max_retries=3,
            circuit_breaker_threshold=5,
            circuit_breaker_timeout=30,
            degradation_mode=True,
        )
        logger.info("Creating RedisHA with URL: %s", url)
    else:
        # Standalone with host/port
        redis_url = f"redis://{host}:{port}/{db}"
        if password:
            redis_url = f"redis://:{password}@{host}:{port}/{db}"
        redis = RedisHA(
            redis_url=redis_url,
            max_retries=3,
            circuit_breaker_threshold=5,
            circuit_breaker_timeout=30,
            degradation_mode=True,
        )
        logger.info("Creating RedisHA with host:port: %s:%s", host, port)
    
    # Start the connection
    try:
        await redis.start()
    except Exception as e:
        logger.error("RedisHA connection failed: %s", e)
        # In degradation mode, redis might still be usable (returns True for writes)
        if not redis._is_connected and not redis.degradation_mode:
            raise
    
    return redis


async def close_redis_client(client) -> None:
    """Close Redis client connection."""
    if hasattr(client, 'close'):
        # For RedisHA wrapper
        await client.close()
    elif hasattr(client, 'aclose'):
        # For raw aioredis client
        await client.aclose()