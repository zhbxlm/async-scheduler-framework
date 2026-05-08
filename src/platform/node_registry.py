"""NodeRegistry — aligned with docs/deepwiki-reference/调度与资源管理.md

Manages cluster machine nodes in Redis:
- Register / heartbeat / mark offline
- IDLE → RESERVED (Lua CAS) → JOINING → JOINED → DRAINING → OFFLINE
- Greedy GPU node selection
- Dead-node detection via heartbeat sorted set
- Lease management

Refactored to inherit BaseRedisRegistry for unified interface.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import orjson

from src.common.error_handling import ExternalServiceError, log_errors
from src.platform.base_registry import BaseRedisRegistry

logger = logging.getLogger(__name__)

_HEARTBEAT_ZSET = "nodes:heartbeat"      # zset score=timestamp
_LEASE_KEY = "node_lease:{node_id}"      # lease TTL key
_NODE_INDEX_KEY = "nodes:all"            # set of all node_ids

# Sentinel tenant for global nodes
_GLOBAL_TENANT = "_global_"


class NodeRegistry(BaseRedisRegistry):
    """Redis-backed registry for cluster machine nodes.

    Inherits BaseRedisRegistry for unified CRUD interface.
    Provides:
    - ``register`` / ``heartbeat`` / ``get_node`` / ``list_all``
    - ``select_nodes`` — greedy GPU selection
    - ``reserve_node`` — Lua CAS IDLE → RESERVED
    - ``mark_joined`` / ``mark_released`` / ``mark_draining`` / ``mark_offline``
    - ``detect_dead_nodes`` — heartbeat timeout scan
    """

    # Lua script: atomic CAS IDLE → RESERVED
    _LUA_RESERVE = """
local key     = KEYS[1]
local lease   = KEYS[2]
local cid     = ARGV[1]
local ttl_ms  = tonumber(ARGV[2])
local now_str = ARGV[3]

local raw = redis.call('GET', key)
if not raw then return {err='node_not_found'} end
local node = cjson.decode(raw)
if node['state'] ~= 'idle' then
    return {err='not_idle', state=node['state']}
end
node['state']      = 'reserved'
node['cluster_id'] = cid
redis.call('SET', key, cjson.encode(node))
redis.call('SET',    lease, cid)
redis.call('PEXPIRE', lease, ttl_ms)
return 'ok'
"""

    def __init__(self, redis_client: Any, *, lease_ttl_ms: int = 30_000) -> None:
        super().__init__(
            redis_client=redis_client,
            key_prefix="node",
            ttl_seconds=0,      # nodes don't expire; use heartbeat detection
        )
        self._lease_ttl_ms = lease_ttl_ms
        # Pre-register Lua script to avoid sending source on every call
        self._reserve_script = redis_client.register_script(self._LUA_RESERVE)

    # ------------------------------------------------------------------
    # Registration & heartbeat
    # ------------------------------------------------------------------

    @log_errors(log_level="ERROR", raise_exception=True, exception_type=ExternalServiceError)
    async def register(self, node_info: Any) -> None:
        """Register or update a node (NodeInfo Pydantic model or dict)."""
        if hasattr(node_info, "model_dump"):
            data = node_info.model_dump()
        else:
            data = dict(node_info)
        node_id = data["node_id"]
        now = time.time()
        data.setdefault("registered_at", now)
        data["last_heartbeat"] = now

        await self.set(_GLOBAL_TENANT, node_id, data)
        await self._r.sadd(_NODE_INDEX_KEY, node_id)
        await self._r.zadd(_HEARTBEAT_ZSET, {node_id: now})
        logger.debug("NodeRegistry.register node_id=%s", node_id)

    @log_errors(log_level="WARNING", raise_exception=False)
    async def heartbeat(
        self, node_id: str, resources: dict[str, Any] | None = None
    ) -> None:
        """Update heartbeat timestamp; optionally refresh resource data."""
        now = time.time()
        data = await self.get(_GLOBAL_TENANT, node_id)
        if data is None:
            return
        data["last_heartbeat"] = now
        if resources:
            data["resources"] = resources
        await self.set(_GLOBAL_TENANT, node_id, data)
        await self._r.zadd(_HEARTBEAT_ZSET, {node_id: now})

    @log_errors(log_level="WARNING", raise_exception=False)
    async def get_node(self, node_id: str) -> Any | None:
        """Return NodeInfo for *node_id*, or None."""
        from src.models.node import NodeInfo  # lazy import
        data = await super().get(_GLOBAL_TENANT, node_id)
        if data is None:
            return None
        return NodeInfo.model_validate(data)

    # Backward-compatible single-arg get
    async def get(self, nid_or_tenant: str, item_id: str | None = None) -> Any | None:  # type: ignore[override]
        """Get node by ID. Supports both get(node_id) and get(tenant, id) forms."""
        if item_id is None:
            return await self.get_node(nid_or_tenant)
        data = await super().get(nid_or_tenant, item_id)
        if data is None:
            return None
        from src.models.node import NodeInfo
        try:
            return NodeInfo.model_validate(data)
        except Exception:
            return data

    @log_errors(log_level="ERROR", raise_exception=False)
    async def list_all(self) -> list[Any]:
        """Return all registered nodes.

        Optimised: pipeline batch-fetches all nodes in one Redis round-trip
        instead of O(n) individual GETs.
        """
        from src.models.node import NodeInfo
        raw_ids = await self._r.smembers(_NODE_INDEX_KEY)
        if not raw_ids:
            return []

        # Decode node IDs and batch GET in one pipeline
        nids = [n.decode() if isinstance(n, bytes) else n for n in raw_ids]
        pipe = self._r.pipeline()
        for nid in nids:
            pipe.get(self._make_key(_GLOBAL_TENANT, nid))
        raws = await pipe.execute()

        nodes: list[Any] = []
        for raw in raws:
            if not raw:
                continue
            try:
                nodes.append(NodeInfo.model_validate(orjson.loads(raw)))
            except Exception:
                pass
        return nodes

    # ------------------------------------------------------------------
    # Node selection (greedy GPU)
    # ------------------------------------------------------------------

    @log_errors(log_level="WARNING", raise_exception=False)
    async def select_nodes(self, required_gpus: int) -> list[Any]:
        """Greedily select IDLE nodes with enough GPU to satisfy *required_gpus*.

        Returns list of NodeInfo sorted by available_gpus descending.
        """
        nodes = await self.list_all()
        idle = [
            n for n in nodes
            if n.state.value == "idle" and n.resources.get_effective_gpus() > 0
        ]
        idle.sort(key=lambda n: n.resources.get_effective_gpus(), reverse=True)

        selected: list[Any] = []
        total = 0
        for node in idle:
            if total >= required_gpus:
                break
            selected.append(node)
            total += node.resources.get_effective_gpus()
        return selected

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    @log_errors(log_level="ERROR", raise_exception=True, exception_type=ExternalServiceError)
    async def reserve_node(self, node_id: str, cluster_id: str) -> bool:
        """Atomically transition node IDLE → RESERVED. Returns True on success."""
        key = self._make_key(_GLOBAL_TENANT, node_id)
        lease_key = _LEASE_KEY.format(node_id=node_id)
        result = await self._reserve_script(
            keys=[key, lease_key],
            args=[cluster_id, str(self._lease_ttl_ms), str(time.time())],
        )
        if result in (b"ok", "ok"):
            logger.debug("NodeRegistry.reserve_node node=%s cluster=%s OK", node_id, cluster_id)
            return True
        logger.warning("NodeRegistry.reserve_node node=%s FAILED result=%s", node_id, result)
        return False

    async def mark_joined(self, node_id: str, ray_node_ip: str = "") -> None:
        await self._update_state(node_id, "joined", ray_node_ip=ray_node_ip)

    async def mark_released(self, node_id: str) -> None:
        """Release a RESERVED node back to IDLE."""
        await self._update_state(node_id, "idle", cluster_id="")
        await self._r.delete(_LEASE_KEY.format(node_id=node_id))

    async def mark_draining(self, node_id: str) -> None:
        await self._update_state(node_id, "draining")

    async def mark_offline(self, node_id: str) -> None:
        await self._update_state(node_id, "offline")
        await self._r.zrem(_HEARTBEAT_ZSET, node_id)

    # ------------------------------------------------------------------
    # Dead node detection
    # ------------------------------------------------------------------

    @log_errors(log_level="WARNING", raise_exception=False)
    async def detect_dead_nodes(self, timeout_seconds: float = 60.0) -> list[str]:
        """Return node_ids whose heartbeat is older than *timeout_seconds*.

        Also transitions them to OFFLINE in Redis.
        """
        cutoff = time.time() - timeout_seconds
        dead_ids_raw = await self._r.zrangebyscore(_HEARTBEAT_ZSET, "-inf", cutoff)
        dead_ids = [
            n.decode() if isinstance(n, bytes) else n
            for n in dead_ids_raw
        ]
        for node_id in dead_ids:
            await self.mark_offline(node_id)
            logger.warning("NodeRegistry: node %s timed out (dead)", node_id)
        return dead_ids

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _update_state(self, node_id: str, state: str, **extra: Any) -> None:
        data = await self.get(_GLOBAL_TENANT, node_id)
        if data is None:
            return
        data["state"] = state
        data.update(extra)
        await self.set(_GLOBAL_TENANT, node_id, data)
