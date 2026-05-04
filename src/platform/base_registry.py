"""BaseRedisRegistry — generic Redis registry with CRUD + TTL."""
from __future__ import annotations
import json
from typing import Generic, TypeVar, Optional
import redis.asyncio as aioredis

T = TypeVar("T")


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

    async def get(self, tenant_id: str, item_id: str) -> Optional[T]:
        data = await self._r.get(self._make_key(tenant_id, item_id))
        if not data:
            return None
        return json.loads(data)

    async def set(self, tenant_id: str, item_id: str, value: T) -> bool:
        key = self._make_key(tenant_id, item_id)
        data = json.dumps(value, ensure_ascii=False)
        if self._ttl > 0:
            await self._r.setex(key, self._ttl, data)
        else:
            await self._r.set(key, data)
        # Update index
        await self._r.sadd(f"{self._prefix}:index:{tenant_id}", item_id)
        await self._r.sadd(f"{self._prefix}:tenants", tenant_id)
        return True

    async def delete(self, tenant_id: str, item_id: str) -> bool:
        key = self._make_key(tenant_id, item_id)
        removed = await self._r.delete(key)
        if removed:
            await self._r.srem(f"{self._prefix}:index:{tenant_id}", item_id)
        return bool(removed)

    async def list(self, tenant_id: str) -> list[str]:
        return [x.decode() if isinstance(x, bytes) else x
                for x in await self._r.smembers(f"{self._prefix}:index:{tenant_id}")]

    async def list_all_tenants(self) -> list[str]:
        return [x.decode() if isinstance(x, bytes) else x
                for x in await self._r.smembers(f"{self._prefix}:tenants")]
