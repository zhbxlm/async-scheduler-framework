"""task-api package-owned tenant registry."""
from __future__ import annotations

import hashlib
import logging
import secrets
from typing import Any

import orjson

from scheduler_ops_api.infra.base_registry import BaseRedisRegistry

logger = logging.getLogger(__name__)
_API_KEYS_HASH = "tenant:api_keys"
_API_KEY_IDX = "tenant:api_keys_idx:{tid}"


class TenantRegistry(BaseRedisRegistry):
    def __init__(self, redis_client: Any, ttl_seconds: int = 0):
        super().__init__(redis_client, "tenant", ttl_seconds)

    async def register(self, tenant_info: dict) -> bool:
        tid = tenant_info.get("tenant_id", "")
        if not tid:
            raise ValueError("tenant_id is required")
        await self.set(tid, tid, tenant_info)
        logger.info("TenantRegistry: registered tenant_id=%s", tid)
        return True

    async def unregister(self, tenant_id: str) -> bool:
        idx_key = _API_KEY_IDX.format(tid=tenant_id)
        key_hashes = await self._r.smembers(idx_key)
        if key_hashes:
            pipe = self._r.pipeline()
            for kh in key_hashes:
                pipe.hdel(_API_KEYS_HASH, kh)
            pipe.delete(idx_key)
            await pipe.execute()
        removed = await self.delete(tenant_id, tenant_id)
        logger.info("TenantRegistry: unregistered tenant_id=%s", tenant_id)
        return removed

    async def get_tenant(self, tenant_id: str) -> dict | None:
        data = await self.get(tenant_id, tenant_id)
        if isinstance(data, str):
            return orjson.loads(data)
        return data

    async def generate_api_key(self, tenant_id: str) -> str:
        raw_key = f"ramu_{secrets.token_urlsafe(32)}"
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        pipe = self._r.pipeline()
        pipe.hset(_API_KEYS_HASH, key_hash, tenant_id)
        pipe.sadd(_API_KEY_IDX.format(tid=tenant_id), key_hash)
        await pipe.execute()
        logger.info("TenantRegistry: generated api_key for tenant_id=%s", tenant_id)
        return raw_key

    async def verify_api_key_hash(self, api_key: str) -> str | None:
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        tid = await self._r.hget(_API_KEYS_HASH, key_hash)
        if tid is None:
            return None
        return tid.decode() if isinstance(tid, bytes) else tid

    async def validate_key(self, api_key: str, tenant_id: str | None = None) -> dict | None:
        tid = await self.verify_api_key_hash(api_key)
        if tid is None:
            return None
        if tenant_id is not None and tid != tenant_id:
            return None
        return await self.get_tenant(tid)
