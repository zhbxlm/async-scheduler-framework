"""TenantRegistry — aligned with docs/deepwiki-reference/配额与多租户.md

Manages tenant CRUD + API key lifecycle:
- register / unregister tenants
- generate_api_key (stored as SHA-256 hash)
- verify_api_key_hash
- revoke_api_key
- get_usage (gpu/task/actor counts from Redis)
"""
from __future__ import annotations

import hashlib
import orjson
import logging
import secrets
from typing import Any

from src.platform.base_registry import BaseRedisRegistry

logger = logging.getLogger(__name__)

_API_KEYS_HASH = "tenant:api_keys"           # hash: sha256 → tenant_id
_API_KEY_IDX = "tenant:api_keys_idx:{tid}"  # set: reverse index per tenant


class TenantRegistry(BaseRedisRegistry):
    """Redis-backed tenant registry with API key management."""

    def __init__(self, redis_client: Any, ttl_seconds: int = 0):
        # ttl=0 → no expiry for tenant records
        super().__init__(redis_client, "tenant", ttl_seconds)

    async def register(self, tenant_info: dict) -> bool:
        """Register or update a tenant."""
        tid = tenant_info.get("tenant_id", "")
        if not tid:
            raise ValueError("tenant_id is required")
        await self.set(tid, tid, tenant_info)
        logger.info("TenantRegistry: registered tenant_id=%s", tid)
        return True

    async def unregister(self, tenant_id: str) -> bool:
        """Unregister tenant and revoke all its API keys."""
        # Revoke all associated API keys
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
        """Fetch tenant info dict."""
        data = await self.get(tenant_id, tenant_id)
        if isinstance(data, str):
            return orjson.loads(data)
        return data

    async def generate_api_key(self, tenant_id: str) -> str:
        """Generate a new API key for tenant, store SHA-256 hash."""
        raw_key = f"ramu_{secrets.token_urlsafe(32)}"
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

        pipe = self._r.pipeline()
        pipe.hset(_API_KEYS_HASH, key_hash, tenant_id)
        pipe.sadd(_API_KEY_IDX.format(tid=tenant_id), key_hash)
        await pipe.execute()

        logger.info("TenantRegistry: generated api_key for tenant_id=%s", tenant_id)
        return raw_key

    async def verify_api_key_hash(self, api_key: str) -> str | None:
        """Return tenant_id if the api_key is valid, else None."""
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        tid = await self._r.hget(_API_KEYS_HASH, key_hash)
        if tid is None:
            return None
        return tid.decode() if isinstance(tid, bytes) else tid

    async def revoke_api_key(self, tenant_id: str, api_key: str) -> bool:
        """Revoke a specific API key."""
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        pipe = self._r.pipeline()
        pipe.hdel(_API_KEYS_HASH, key_hash)
        pipe.srem(_API_KEY_IDX.format(tid=tenant_id), key_hash)
        results = await pipe.execute()
        return bool(results[0])

    async def get_usage(self, tenant_id: str) -> dict:
        """Return current resource usage for tenant."""
        usage_key = f"tenant_usage:{tenant_id}"
        pipe = self._r.pipeline()
        pipe.hget(usage_key, "gpu_count")
        pipe.hget(usage_key, "task_count")
        pipe.hget(usage_key, "actor_count")
        results = await pipe.execute()
        return {
            "gpu_count": int(results[0] or 0),
            "task_count": int(results[1] or 0),
            "actor_count": int(results[2] or 0),
        }

    async def validate_key(self, api_key: str, tenant_id: str | None = None) -> dict | None:
        """Validate API key; return tenant context dict or None."""
        tid = await self.verify_api_key_hash(api_key)
        if tid is None:
            return None
        if tenant_id is not None and tid != tenant_id:
            return None
        tenant_data = await self.get_tenant(tid)
        return tenant_data
