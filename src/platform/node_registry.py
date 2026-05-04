"""NodeRegistry — aligned with docs/deepwiki-reference/调度与资源管理.md

Manages cluster machine nodes in Redis:
- Register / heartbeat / mark offline
- IDLE → RESERVED (Lua CAS) → JOINING → JOINED → DRAINING → OFFLINE
- Greedy GPU node selection
- Dead-node detection via heartbeat sorted set
- Lease management
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

# Redis key helpers
_NODE_KEY = "node:{node_id}"                      # node JSON hash
_HEARTBEAT_ZSET = "nodes:heartbeat"               # zset score=timestamp
_LEASE_KEY = "node_lease:{node_id}"               # lease TTL key
_NODE_INDEX_KEY = "nodes:all"                     # set of all node_ids


class NodeRegistry:
    """Redis-backed registry for cluster machine nodes.

    All state is persisted in Redis.  Provides:
    - ``register`` / ``heartbeat`` / ``get`` / ``list_all``
    - ``select_nodes`` — greedy GPU selection
    - ``reserve_node`` — Lua CAS IDLE→RESERVED
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
        self._r = redis_client
        self._lease_ttl_ms = lease_ttl_ms

    # ------------------------------------------------------------------
    # Registration & heartbeat
    # ------------------------------------------------------------------

    async def register(self, node_info: Any) -> None:
        """Register or update a node (NodeInfo Pydantic model or dict)."""
        from src.models.node import NodeInfo as NodeInfoModel  # lazy import
        if hasattr(node_info, "model_dump"):
            data = node_info.model_dump()
        else:
            data = dict(node_info)
        node_id = data["node_id"]
        now = time.time()
        data.setdefault("registered_at", now)
        data["last_heartbeat"] = now

        key = _NODE_KEY.format(node_id=node_id)
        await self._r.set(key, json.dumps(data))
        await self._r.sadd(_NODE_INDEX_KEY, node_id)
        await self._r.zadd(_HEARTBEAT_ZSET, {node_id: now})
        logger.debug("NodeRegistry.register node_id=%s", node_id)

    async def heartbeat(self, node_id: str, resources: dict[str, Any] | None = None) -> None:
        """Update heartbeat timestamp; optionally refresh resource data."""
        now = time.time()
        key = _NODE_KEY.format(node_id=node_id)
        raw = await self._r.get(key)
        if raw is None:
            return
        data = json.loads(raw)
        data["last_heartbeat"] = now
        if resources:
            data["resources"] = resources
        await self._r.set(key, json.dumps(data))
        await self._r.zadd(_HEARTBEAT_ZSET, {node_id: now})

    async def get(self, node_id: str) -> Any | None:
        """Return NodeInfo for *node_id*, or None if not found."""
        from src.models.node import NodeInfo  # lazy import
        key = _NODE_KEY.format(node_id=node_id)
        raw = await self._r.get(key)
        if raw is None:
            return None
        return NodeInfo.model_validate(json.loads(raw))

    async def list_all(self) -> list[Any]:
        """Return all registered nodes."""
        from src.models.node import NodeInfo  # lazy import
        node_ids = await self._r.smembers(_NODE_INDEX_KEY)
        nodes = []
        for nid in node_ids:
            raw = await self._r.get(_NODE_KEY.format(node_id=nid))
            if raw:
                try:
                    nodes.append(NodeInfo.model_validate(json.loads(raw)))
                except Exception:
                    pass
        return nodes

    # ------------------------------------------------------------------
    # Node selection (greedy GPU)
    # ------------------------------------------------------------------

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

    async def reserve_node(self, node_id: str, cluster_id: str) -> bool:
        """Atomically transition node IDLE → RESERVED.  Returns True on success."""
        key = _NODE_KEY.format(node_id=node_id)
        lease_key = _LEASE_KEY.format(node_id=node_id)
        result = await self._r.eval(
            self._LUA_RESERVE,
            2,
            key, lease_key,
            cluster_id, str(self._lease_ttl_ms), str(time.time()),
        )
        if result == b"ok" or result == "ok":
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

    async def detect_dead_nodes(self, timeout_seconds: float = 60.0) -> list[str]:
        """Return node_ids whose heartbeat is older than *timeout_seconds*.

        Also transitions them to OFFLINE in Redis.
        """
        cutoff = time.time() - timeout_seconds
        dead_ids_raw = await self._r.zrangebyscore(_HEARTBEAT_ZSET, "-inf", cutoff)
        dead_ids = [n.decode() if isinstance(n, bytes) else n for n in dead_ids_raw]
        for node_id in dead_ids:
            await self.mark_offline(node_id)
            logger.warning("NodeRegistry: node %s timed out (dead)", node_id)
        return dead_ids

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _update_state(self, node_id: str, state: str, **extra: Any) -> None:
        key = _NODE_KEY.format(node_id=node_id)
        raw = await self._r.get(key)
        if raw is None:
            return
        data = json.loads(raw)
        data["state"] = state
        data.update(extra)
        await self._r.set(key, json.dumps(data))
