"""ClusterRegistry — aligned with docs/deepwiki-reference/调度与资源管理.md

Redis-backed cluster registry with multi-dimensional scoring for routing.
Maintains PlannedResources + ObservedResources dual-layer model.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from src.models.cluster import ClusterInfo, ClusterResources, ClusterStatus, ObservedResources

logger = logging.getLogger(__name__)

_CLUSTER_KEY = "cluster:{cluster_id}"
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


class ClusterRegistry:
    """Redis-backed registry for Ray clusters."""

    def __init__(self, redis_client: Any) -> None:
        self._r = redis_client

    async def register(self, cluster: ClusterInfo) -> None:
        key = _CLUSTER_KEY.format(cluster_id=cluster.cluster_id)
        await self._r.set(key, cluster.model_dump_json())
        await self._r.sadd(_CLUSTER_INDEX, cluster.cluster_id)
        logger.info("ClusterRegistry.register cluster_id=%s", cluster.cluster_id)

    async def get(self, cluster_id: str) -> ClusterInfo | None:
        key = _CLUSTER_KEY.format(cluster_id=cluster_id)
        raw = await self._r.get(key)
        if raw is None:
            return None
        return ClusterInfo.model_validate(json.loads(raw))

    async def list_all(self) -> list[ClusterInfo]:
        ids = await self._r.smembers(_CLUSTER_INDEX)
        clusters = []
        for cid in ids:
            c = await self.get(cid.decode() if isinstance(cid, bytes) else cid)
            if c:
                clusters.append(c)
        return clusters

    async def unregister(self, cluster_id: str) -> None:
        await self._r.delete(_CLUSTER_KEY.format(cluster_id=cluster_id))
        await self._r.srem(_CLUSTER_INDEX, cluster_id)

    async def update_resources(
        self, cluster_id: str, observed: ObservedResources
    ) -> None:
        """Atomically update the observed resource layer for *cluster_id*."""
        key = _CLUSTER_KEY.format(cluster_id=cluster_id)
        fields = {
            "total_gpus": observed.total_gpus,
            "available_gpus": observed.available_gpus,
            "available_cpus": observed.available_cpus,
            "available_memory_gb": observed.available_memory_gb,
        }
        await self._r.eval(_LUA_UPDATE_CLUSTER, 1, key, json.dumps(fields))

    async def update_status(self, cluster_id: str, status: ClusterStatus) -> None:
        key = _CLUSTER_KEY.format(cluster_id=cluster_id)
        await self._r.eval(
            _LUA_UPDATE_CLUSTER, 1, key,
            json.dumps({"status": status.value})
        )

    async def select_cluster(
        self,
        required_gpus: int = 0,
        capability: str | None = None,
        tenant_id: str | None = None,
    ) -> ClusterInfo | None:
        """Select the best cluster using multi-dimensional scoring.

        Scoring: available_gpus DESC, then available_cpus DESC.
        Filters: ACTIVE status, required capability, tenant.
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

        # Score: (available_gpus, available_cpus) descending
        candidates.sort(
            key=lambda c: (c.resources.available_gpus, c.resources.available_cpus),
            reverse=True,
        )
        return candidates[0]
