"""ResourceManager — aligned with docs/deepwiki-reference/调度与资源管理.md

4-phase node allocation: SELECT -> RESERVE -> INVITE -> CONFIRM
Auto-scaling decision with cooldown Lua CAS
Maintenance chain: heartbeat timeout -> lease cleanup -> resource recompute -> health sync
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from src.models.cluster import ClusterInfo, ObservedResources
from src.models.node import NodeInfo, NodeState

logger = logging.getLogger(__name__)

_LUA_SCALING_COOLDOWN = """
local key     = KEYS[1]
local now     = tonumber(ARGV[1])
local cooldown= tonumber(ARGV[2])
local last    = tonumber(redis.call('GET', key) or '0')
if now - last < cooldown then
    return 'cooling'
end
redis.call('SET', key, tostring(now))
redis.call('EXPIRE', key, cooldown * 2)
return 'ok'
"""

_SCALING_COOLDOWN_KEY = "scaling_cooldown:{capability}"


class AgentClient:
    """HTTP client for sending commands to Node Agents."""

    def __init__(self, timeout_seconds: float = 15.0) -> None:
        self._timeout = timeout_seconds

    async def invite(self, node: NodeInfo, cluster: ClusterInfo) -> bool:
        import httpx
        url = node.agent_url + "/invite"
        payload = {
            "cluster_id": cluster.cluster_id,
            "ray_head_address": cluster.ray_head_address,
            "labels": node.labels,
            "custom_resources": node.custom_resources,
        }
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(url, json=payload)
                resp.raise_for_status()
                data = resp.json()
                return bool(data.get("accepted"))
        except Exception as e:
            logger.error("AgentClient.invite node=%s failed: %s", node.node_id, e)
            return False

    async def release(self, node: NodeInfo, cluster_id: str, graceful: bool = True) -> bool:
        import httpx
        url = node.agent_url + "/release"
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(url, json={"cluster_id": cluster_id, "graceful": graceful})
                return resp.status_code < 400
        except Exception:
            return False


class ResourceManager:
    """4-phase node allocation + autoscaling + maintenance."""

    def __init__(
        self,
        node_registry: Any,
        cluster_registry: Any,
        capability_registry: Any,
        queue_manager: Any,
        redis_client: Any,
        agent_client: AgentClient | None = None,
        *,
        scaling_up_threshold: float = 0.8,
        scaling_down_threshold: float = 0.3,
        cooldown_seconds: int = 60,
        max_scaling_increment: int = 4,
        min_actors: int = 1,
        heartbeat_timeout_seconds: float = 60.0,
    ) -> None:
        self._nodes = node_registry
        self._clusters = cluster_registry
        self._caps = capability_registry
        self._queue = queue_manager
        self._r = redis_client
        self._agent = agent_client or AgentClient()
        self._up_threshold = scaling_up_threshold
        self._down_threshold = scaling_down_threshold
        self._cooldown = cooldown_seconds
        self._max_increment = max_scaling_increment
        self._min_actors = min_actors
        self._hb_timeout = heartbeat_timeout_seconds

    async def allocate_nodes(self, cluster: ClusterInfo, required_gpus: int) -> list[NodeInfo]:
        """SELECT -> RESERVE -> INVITE -> CONFIRM. Returns joined nodes."""
        candidates = await self._nodes.select_nodes(required_gpus)
        if not candidates:
            return []

        reserved = []
        for node in candidates:
            ok = await self._nodes.reserve_node(node.node_id, cluster.cluster_id)
            if ok:
                reserved.append(node)
                if sum(n.resources.get_effective_gpus() for n in reserved) >= required_gpus:
                    break

        if not reserved:
            return []

        results = await asyncio.gather(
            *[self._agent.invite(n, cluster) for n in reserved],
            return_exceptions=True,
        )

        joined = []
        for node, result in zip(reserved, results):
            if result is True:
                await self._nodes.mark_joined(node.node_id)
                joined.append(node)
            else:
                await self._nodes.mark_released(node.node_id)

        if joined:
            await self._recompute_cluster_resources(cluster.cluster_id)
        return joined

    async def evaluate_scaling(self, capability: str, current_size: int) -> int | None:
        """Return new target size or None if no change."""
        if self._queue is None:
            return None
        snapshot = await self._queue.get_queue_snapshot(capability)
        pending = snapshot.get("pending", 0)
        running = snapshot.get("running", 0)
        utilization = running / max(1, current_size)

        should_up = utilization >= self._up_threshold or (pending / max(1, current_size)) >= 2.0
        should_down = (
            utilization < self._down_threshold
            and pending == 0
            and current_size > self._min_actors
        )

        if should_up:
            target = min(current_size * 2, current_size + self._max_increment) if current_size else 1
            if await self._check_cooldown(capability):
                return target
        elif should_down:
            target = max(self._min_actors, current_size // 2)
            if await self._check_cooldown(capability):
                return target
        return None

    async def _check_cooldown(self, capability: str) -> bool:
        key = _SCALING_COOLDOWN_KEY.format(capability=capability)
        result = await self._r.eval(_LUA_SCALING_COOLDOWN, 1, key,
                                    str(time.time()), str(self._cooldown))
        return (result.decode() if isinstance(result, bytes) else str(result)) == "ok"

    async def run_maintenance(self) -> dict[str, Any]:
        """Heartbeat timeout -> recompute resources -> health sync."""
        summary: dict[str, Any] = {"dead_nodes": [], "clusters_updated": 0, "caps_health_updated": 0}
        dead = await self._nodes.detect_dead_nodes(self._hb_timeout)
        summary["dead_nodes"] = dead

        all_nodes = await self._nodes.list_all()
        node_map = {n.node_id: n for n in all_nodes}
        affected: set[str] = {
            node_map[d].cluster_id for d in dead
            if d in node_map and node_map[d].cluster_id
        }
        for cid in affected:
            await self._recompute_cluster_resources(cid)
            summary["clusters_updated"] += 1

        caps = await self._caps.list_all()
        for cap in caps:
            cluster = await self._clusters.get(cap.cluster_id) if cap.cluster_id else None
            if cluster:
                alive = sum(
                    1 for n in all_nodes
                    if n.cluster_id == cluster.cluster_id
                    and n.state not in (NodeState.OFFLINE, NodeState.DRAINING)
                )
                if alive == 0 and cap.health_status.value != "unhealthy":
                    await self._caps.update_health(cap.capability_name, success=False, unhealthy_threshold=1)
                    summary["caps_health_updated"] += 1
                elif alive > 0 and cap.health_status.value == "unhealthy":
                    await self._caps.update_health(cap.capability_name, success=True, healthy_threshold=1)
                    summary["caps_health_updated"] += 1
        return summary

    async def _recompute_cluster_resources(self, cluster_id: str) -> None:
        all_nodes = await self._nodes.list_all()
        joined = [n for n in all_nodes if n.cluster_id == cluster_id and n.state == NodeState.JOINED]
        observed = ObservedResources(
            total_gpus=sum(n.resources.get_effective_gpus() for n in joined),
            available_gpus=sum(n.resources.get_effective_gpus() for n in joined),
            available_cpus=sum(n.resources.get_effective_cpus() for n in joined),
            available_memory_gb=round(sum(n.resources.get_effective_memory_mb() / 1024 for n in joined), 2),
        )
        await self._clusters.update_resources(cluster_id, observed)

    async def confirm(self, node: NodeInfo, cluster: ClusterInfo) -> bool:
        """Phase 4: CONFIRM — mark node as fully JOINED in registry."""
        await self._registry.set_node_state(node.node_id, "JOINED")
        return True

