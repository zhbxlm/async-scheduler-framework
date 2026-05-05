"""BaseRedisRegistry — generic Redis registry with CRUD + TTL.

Uses orjson for 2-5x faster serialisation than stdlib json.
"""
from __future__ import annotations
import logging
from typing import Generic, TypeVar, Optional, Any
import redis.asyncio as aioredis
import orjson

from src.common.error_handling import log_errors, ExternalServiceError

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _json_dumps(value: Any) -> bytes:
    """Fast JSON encode using orjson. Returns bytes (Redis-friendly)."""
    return orjson.dumps(value)


def _json_loads(raw: bytes | str | None) -> Any:
    """Fast JSON decode using orjson. Handles empty/None input."""
    if not raw:
        return None
    return orjson.loads(raw)


class BaseRedisRegistry(Generic[T]):
    """Base registry with Redis storage.

    Provides standard CRUD operations with tenant isolation.
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        key_prefix: str,
        ttl_seconds: int = 0,
    ):
        self._r = redis_client
        self._prefix = key_prefix.rstrip(":")
        self._ttl = ttl_seconds
        self._logger = logging.getLogger(self.__class__.__module__)

    def _make_key(self, tenant_id: str, item_id: str) -> str:
        return f"{self._prefix}:{tenant_id}:{item_id}"

    @log_errors(log_level="ERROR", raise_exception=False, exception_type=ExternalServiceError)
    async def get(self, tenant_id: str, item_id: str) -> Optional[T]:
        key = self._make_key(tenant_id, item_id)
        try:
            data = await self._r.get(key)
            if not data:
                return None
            return _json_loads(data)
        except orjson.JSONDecodeError as e:
            self._logger.error("JSON decode error for key %s: %s", key, e)
            return None

    @log_errors(log_level="ERROR", raise_exception=True, exception_type=ExternalServiceError)
    async def set(self, tenant_id: str, item_id: str, value: T) -> bool:
        key = self._make_key(tenant_id, item_id)
        data = _json_dumps(value)
        if self._ttl > 0:
            await self._r.setex(key, self._ttl, data)
        else:
            await self._r.set(key, data)
        # Update index
        await self._r.sadd(f"{self._prefix}:index:{tenant_id}", item_id)
        await self._r.sadd(f"{self._prefix}:tenants", tenant_id)
        self._logger.debug("Set %s:%s for tenant %s", self._prefix, item_id, tenant_id)
        return True

    @log_errors(log_level="ERROR", raise_exception=False, exception_type=ExternalServiceError)
    async def delete(self, tenant_id: str, item_id: str) -> bool:
        key = self._make_key(tenant_id, item_id)
        removed = await self._r.delete(key)
        if removed:
            await self._r.srem(f"{self._prefix}:index:{tenant_id}", item_id)
            self._logger.debug("Deleted %s:%s for tenant %s", self._prefix, item_id, tenant_id)
        return bool(removed)

    @log_errors(log_level="ERROR", raise_exception=False, exception_type=ExternalServiceError)
    async def list(self, tenant_id: str) -> list[str]:
        members = await self._r.smembers(f"{self._prefix}:index:{tenant_id}")
        return [x.decode() if isinstance(x, bytes) else x for x in members]

    @log_errors(log_level="ERROR", raise_exception=False, exception_type=ExternalServiceError)
    async def list_all_tenants(self) -> list[str]:
        members = await self._r.smembers(f"{self._prefix}:tenants")
        return [x.decode() if isinstance(x, bytes) else x for x in members]
