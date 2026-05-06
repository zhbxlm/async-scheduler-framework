"""ClusterRegistry — aligned with docs/deepwiki-reference/调度与资源管理.md

Redis-backed cluster registry with multi-dimensional scoring for routing.
Maintains PlannedResources + ObservedResources dual-layer model.

Refactored to inherit BaseRedisRegistry for unified interface.
"""
from __future__ import annotations

import orjson
import logging
from typing import Any

from src.models.cluster import ClusterInfo, ClusterStatus, ObservedResources
from src.platform.base_registry import BaseRedisRegistry
from src.common.error_handling import log_errors, ExternalServiceError

logger = logging.getLogger(__name__)

_CLUSTER_INDEX = "clusters:all"

# Lua: atomic field update on cluster JSON
_LUA_UPDATE_CLUSTER = """
local key    = KEYS[1]
local fields = cjson.decode(ARGV[1])
local raw    = redis.call('GET', key)
if not raw then return {err='not_found'} end
local obj = cjson.decode(raw)
for k, v in pairs(fields) do
    obj[k] = v
end
redis.call('SET', key, cjson.encode(obj))
return 'ok'
"""

# Sentinel tenant for global clusters
_GLOBAL_TENANT = "_global_"


class ClusterRegistry(BaseRedisRegistry):
    """Redis-backed registry for Ray clusters.

    Inherits BaseRedisRegistry for unified CRUD interface.
    Clusters are global resources (not tenant-scoped at storage level),
    though filtering by tenant_id is supported at query time.
    """

    def __init__(self, redis_client: Any) -> None:
        super().__init__(
            redis_client=redis_client,
            key_prefix="cluster",
            ttl_seconds=0,      # clusters don't expire
        )
        self._lua_update = redis_client.register_script(_LUA_UPDATE_CLUSTER)
    # ------------------------------------------------------------------
    # High-level API
    # ------------------------------------------------------------------

    @log_errors(log_level="ERROR", raise_exception=True, exception_type=ExternalServiceError)
    async def register(self, cluster: ClusterInfo) -> None:
        """Register or update a cluster."""
        await self.set(_GLOBAL_TENANT, cluster.cluster_id, cluster.model_dump())
        await self._r.sadd(_CLUSTER_INDEX, cluster.cluster_id)
        logger.info("ClusterRegistry.register cluster_id=%s", cluster.cluster_id)

    @log_errors(log_level="WARNING", raise_exception=False)
    async def get_cluster(self, cluster_id: str) -> ClusterInfo | None:
        """Get a cluster by ID."""
        data = await super().get(_GLOBAL_TENANT, cluster_id)
        if data is None:
            return None
        return ClusterInfo.model_validate(data)

    # Backward-compatible single-arg get
    async def get(self, cid_or_tenant: str, item_id: str | None = None) -> ClusterInfo | None:  # type: ignore[override]
        """Get cluster by ID. Supports both get(cluster_id) and get(tenant, id) forms."""
        if item_id is None:
            return await self.get_cluster(cid_or_tenant)
        data = await super().get(cid_or_tenant, item_id)
        if data is None:
            return None
        return ClusterInfo.model_validate(data)

    @log_errors(log_level="ERROR", raise_exception=False)
    async def list_all(self) -> list[ClusterInfo]:
        """Return all registered clusters."""
        ids = await self._r.smembers(_CLUSTER_INDEX)
        clusters: list[ClusterInfo] = []
        for raw_id in ids:
            cid = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
            c = await self.get_cluster(cid)
            if c:
                clusters.append(c)
        return clusters

    @log_errors(log_level="ERROR", raise_exception=True, exception_type=ExternalServiceError)
    async def unregister(self, cluster_id: str) -> None:
        """Remove a cluster from registry."""
        await self.delete(_GLOBAL_TENANT, cluster_id)
        await self._r.srem(_CLUSTER_INDEX, cluster_id)

    @log_errors(log_level="ERROR", raise_exception=True, exception_type=ExternalServiceError)
    async def update_resources(
        self, cluster_id: str, observed: ObservedResources
    ) -> None:
        """Atomically update the observed resource layer for *cluster_id*."""
        key = self._make_key(_GLOBAL_TENANT, cluster_id)
        fields = {
            "total_gpus": observed.total_gpus,
            "available_gpus": observed.available_gpus,
            "available_cpus": observed.available_cpus,
            "available_memory_gb": observed.available_memory_gb,
        }
        await self._lua_update(keys=[key], args=[orjson.dumps(fields)])

    @log_errors(log_level="ERROR", raise_exception=True, exception_type=ExternalServiceError)
    async def update_status(self, cluster_id: str, status: ClusterStatus) -> None:
        """Update cluster status."""
        key = self._make_key(_GLOBAL_TENANT, cluster_id)
        await self._lua_update(
            keys=[key],
            args=[orjson.dumps({"status": status.value})]
        )

    @log_errors(log_level="WARNING", raise_exception=False)
    async def select_cluster(
        self,
        required_gpus: int = 0,
        capability: str | None = None,
        tenant_id: str | None = None,
    ) -> ClusterInfo | None:
        """Select the best cluster using multi-dimensional scoring.

        Scoring: available_gpus DESC, then available_cpus DESC.
        Filters: ACTIVE status, required capability, optional tenant.
        """
        clusters = await self.list_all()
        candidates = []
        for c in clusters:
            if c.status != ClusterStatus.ACTIVE:
                continue
            if required_gpus > 0 and c.resources.available_gpus < required_gpus:
                continue
            if capability and capability not in c.capabilities:
                continue
            if tenant_id and c.tenant_id not in ("", tenant_id):
                continue
            candidates.append(c)

        if not candidates:
            return None

        candidates.sort(
            key=lambda c: (c.resources.available_gpus, c.resources.available_cpus),
            reverse=True,
        )
        return candidates[0]
