"""TenantRegistry — API key validation + quota management."""
from __future__ import annotations
from src.platform.base_registry import BaseRedisRegistry


class TenantRegistry(BaseRedisRegistry):
    def __init__(self, redis_client, ttl_seconds: int = 86400):
        super().__init__(redis_client, "tenants", ttl_seconds)

    async def validate_key(self, api_key: str, tenant_id: str | None) -> dict | None:
        """Return tenant context if API key valid."""
        # TODO: implement validation
        return None
