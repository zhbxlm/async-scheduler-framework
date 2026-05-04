"""ClusterRegistry: multi-cluster routing for task dispatch.

Implements G12 from the deepwiki P2 roadmap:
- Maintains a registry of known clusters and their capabilities
- Routes tasks to the best-fit cluster based on:
    * Required capabilities
    * Cluster health / availability
    * Current load (pending queue depth)
    * Affinity rules (prefer / require cluster tags)
- Supports soft-affinity (prefer) and hard-affinity (require) routing
- Redis-backed for cross-process cluster state; in-process fallback for tests

Cluster state is stored at: cluster:registry:{cluster_id}  (Hash)
Cluster capability index:   cluster:capabilities:{capability}  (Set of cluster_ids)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

__all__ = [
    "ClusterHealth",
    "ClusterInfo",
    "RoutingPolicy",
    "ClusterRegistry",
]

_NS = "cluster"


class ClusterHealth(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


@dataclass
class ClusterInfo:
    """Metadata and runtime state for a single cluster."""
    cluster_id: str
    name: str
    capabilities: list[str] = field(default_factory=list)
    tags: dict[str, str] = field(default_factory=dict)
    endpoint: str | None = None           # API endpoint for cross-cluster dispatch
    health: ClusterHealth = ClusterHealth.HEALTHY
    pending_tasks: int = 0
    max_tasks: int = 1000
    last_heartbeat_at: float = field(default_factory=time.time)
    region: str = "default"
    priority: int = 0                    # Lower = preferred

    @property
    def utilization(self) -> float:
        return self.pending_tasks / max(1, self.max_tasks)

    @property
    def is_available(self) -> bool:
        return self.health != ClusterHealth.UNAVAILABLE

    def to_dict(self) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "name": self.name,
            "capabilities": self.capabilities,
            "tags": self.tags,
            "endpoint": self.endpoint,
            "health": self.health.value,
            "pending_tasks": self.pending_tasks,
            "max_tasks": self.max_tasks,
            "utilization": round(self.utilization, 3),
            "last_heartbeat_at": self.last_heartbeat_at,
            "region": self.region,
            "priority": self.priority,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ClusterInfo":
        data = dict(data)
        if "health" in data:
            data["health"] = ClusterHealth(data["health"])
        return cls(**{k: v for k, v in data.items() if hasattr(cls, k)})


@dataclass
class RoutingPolicy:
    """Policy for routing a task to a cluster."""
    required_capabilities: list[str] = field(default_factory=list)
    preferred_capabilities: list[str] = field(default_factory=list)
    required_tags: dict[str, str] = field(default_factory=dict)    # must match all
    preferred_tags: dict[str, str] = field(default_factory=dict)   # bonus score
    preferred_region: str | None = None
    exclude_clusters: list[str] = field(default_factory=list)
    max_utilization: float = 0.9


class ClusterRegistry:
    """Registry for multi-cluster routing decisions.

    In Redis-backed mode, cluster state is shared across all nodes.
    In in-process mode, state is local (useful for testing).

    Usage::

        registry = ClusterRegistry(redis_client=redis)
        await registry.register_cluster(ClusterInfo(
            cluster_id="gpu-cluster-1",
            capabilities=["gpu", "ml-inference"],
            tags={"env": "prod"},
            region="us-west",
        ))

        best = await registry.route_task(RoutingPolicy(
            required_capabilities=["gpu"],
            preferred_region="us-west",
        ))
        # best is a ClusterInfo or None
    """

    def __init__(self, redis_client: Any | None = None) -> None:
        self._redis = redis_client
        self._local: dict[str, ClusterInfo] = {}
        self._lock = asyncio.Lock()
        self._routing_decisions: deque[dict[str, Any]] = deque(maxlen=50)

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    async def register_cluster(self, cluster: ClusterInfo) -> None:
        """Register or update a cluster."""
        if self._redis is not None:
            key = f"{_NS}:registry:{cluster.cluster_id}"
            await self._redis.hset(key, mapping={
                k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                for k, v in cluster.to_dict().items()
            })
            # Update capability index
            for cap in cluster.capabilities:
                await self._redis.sadd(f"{_NS}:capabilities:{cap}", cluster.cluster_id)
            await self._redis.sadd(f"{_NS}:all_clusters", cluster.cluster_id)
        else:
            async with self._lock:
                self._local[cluster.cluster_id] = cluster

    async def deregister_cluster(self, cluster_id: str) -> None:
        """Remove a cluster from the registry."""
        if self._redis is not None:
            key = f"{_NS}:registry:{cluster_id}"
            info = await self._get_cluster_redis(cluster_id)
            if info:
                for cap in info.capabilities:
                    await self._redis.srem(f"{_NS}:capabilities:{cap}", cluster_id)
            await self._redis.delete(key)
            await self._redis.srem(f"{_NS}:all_clusters", cluster_id)
        else:
            async with self._lock:
                self._local.pop(cluster_id, None)

    async def update_heartbeat(
        self,
        cluster_id: str,
        pending_tasks: int | None = None,
        health: ClusterHealth | None = None,
    ) -> None:
        """Update a cluster's runtime state."""
        if self._redis is not None:
            key = f"{_NS}:registry:{cluster_id}"
            updates: dict[str, str] = {"last_heartbeat_at": str(time.time())}
            if pending_tasks is not None:
                updates["pending_tasks"] = str(pending_tasks)
            if health is not None:
                updates["health"] = health.value
            await self._redis.hset(key, mapping=updates)
        else:
            async with self._lock:
                info = self._local.get(cluster_id)
                if info:
                    info.last_heartbeat_at = time.time()
                    if pending_tasks is not None:
                        info.pending_tasks = pending_tasks
                    if health is not None:
                        info.health = health

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    async def route_task(self, policy: RoutingPolicy) -> ClusterInfo | None:
        """Find the best cluster for a task given the routing policy.

        Returns None if no suitable cluster is found.
        """
        candidates = await self._get_all_clusters()

        # Filter: exclude list
        candidates = [c for c in candidates if c.cluster_id not in policy.exclude_clusters]

        # Filter: must be available
        candidates = [c for c in candidates if c.is_available]

        # Filter: max utilization
        candidates = [c for c in candidates if c.utilization <= policy.max_utilization]

        # Filter: required capabilities (all must match)
        if policy.required_capabilities:
            candidates = [
                c for c in candidates
                if all(cap in c.capabilities for cap in policy.required_capabilities)
            ]

        # Filter: required tags (all must match)
        if policy.required_tags:
            candidates = [
                c for c in candidates
                if all(c.tags.get(k) == v for k, v in policy.required_tags.items())
            ]

        if not candidates:
            return None

        # Scoring: lower = better
        def score(c: ClusterInfo) -> float:
            s = c.utilization * 100  # base: utilization
            s += c.priority * 10    # cluster priority
            # Preferred capabilities bonus
            cap_bonus = sum(
                5 for cap in policy.preferred_capabilities if cap in c.capabilities
            )
            s -= cap_bonus
            # Preferred tags bonus
            tag_bonus = sum(
                3 for k, v in policy.preferred_tags.items() if c.tags.get(k) == v
            )
            s -= tag_bonus
            # Region bonus
            if policy.preferred_region and c.region == policy.preferred_region:
                s -= 10
            return s

        selected = min(candidates, key=score)
        self._routing_decisions.append({
            "timestamp": time.time(),
            "selected_cluster_id": selected.cluster_id,
            "required_capabilities": list(policy.required_capabilities),
            "preferred_region": policy.preferred_region,
            "candidate_count": len(candidates),
        })
        return selected

    async def list_clusters(self) -> list[ClusterInfo]:
        """Return all registered clusters."""
        return await self._get_all_clusters()

    async def get_cluster(self, cluster_id: str) -> ClusterInfo | None:
        """Get a specific cluster by ID."""
        if self._redis is not None:
            return await self._get_cluster_redis(cluster_id)
        async with self._lock:
            return self._local.get(cluster_id)

    async def get_clusters_for_capability(self, capability: str) -> list[ClusterInfo]:
        """Return clusters that support a given capability."""
        if self._redis is not None:
            cluster_ids = await self._redis.smembers(f"{_NS}:capabilities:{capability}")
            results = []
            for cid in cluster_ids:
                info = await self._get_cluster_redis(cid)
                if info:
                    results.append(info)
            return results
        async with self._lock:
            return [c for c in self._local.values() if capability in c.capabilities]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_all_clusters(self) -> list[ClusterInfo]:
        if self._redis is not None:
            cluster_ids = await self._redis.smembers(f"{_NS}:all_clusters")
            results = []
            for cid in cluster_ids:
                info = await self._get_cluster_redis(cid)
                if info:
                    results.append(info)
            return results
        async with self._lock:
            return list(self._local.values())

    async def _get_cluster_redis(self, cluster_id: str) -> ClusterInfo | None:
        key = f"{_NS}:registry:{cluster_id}"
        raw = await self._redis.hgetall(key)
        if not raw:
            return None
        data: dict[str, Any] = {}
        for k, v in raw.items():
            try:
                data[k] = json.loads(v)
            except (json.JSONDecodeError, ValueError):
                data[k] = v
        return ClusterInfo.from_dict(data)

    async def get_stats_async(self) -> dict[str, Any]:
        clusters = await self._get_all_clusters()
        return self._build_stats(clusters)

    def get_stats(self) -> dict[str, Any]:
        return self._build_stats(list(self._local.values()))

    def _build_stats(self, clusters: list[ClusterInfo]) -> dict[str, Any]:
        health_counts = {
            ClusterHealth.HEALTHY.value: 0,
            ClusterHealth.DEGRADED.value: 0,
            ClusterHealth.UNAVAILABLE.value: 0,
        }
        capability_counts: dict[str, int] = {}
        region_counts: dict[str, int] = {}
        total_pending = 0
        capacity_total = 0
        capacity_used_estimate = 0
        capability_workload: dict[str, dict[str, Any]] = {}

        for cluster in clusters:
            health_counts[cluster.health.value] = health_counts.get(cluster.health.value, 0) + 1
            region_counts[cluster.region] = region_counts.get(cluster.region, 0) + 1
            total_pending += cluster.pending_tasks
            capacity_total += cluster.max_tasks
            used_estimate = min(cluster.pending_tasks, cluster.max_tasks)
            capacity_used_estimate += used_estimate

            for capability in cluster.capabilities:
                capability_counts[capability] = capability_counts.get(capability, 0) + 1
                bucket = capability_workload.setdefault(capability, {
                    "cluster_count": 0,
                    "pending": 0,
                    "capacity_total": 0,
                    "capacity_used_estimate": 0,
                    "regions": {},
                })
                bucket["cluster_count"] += 1
                bucket["pending"] += cluster.pending_tasks
                bucket["capacity_total"] += cluster.max_tasks
                bucket["capacity_used_estimate"] += used_estimate
                bucket["regions"][cluster.region] = bucket["regions"].get(cluster.region, 0) + 1

        return {
            "redis_backed": self._redis is not None,
            "cluster_count": len(clusters),
            "local_cluster_count": len(self._local),
            "health_counts": health_counts,
            "capability_counts": capability_counts,
            "region_counts": region_counts,
            "total_pending_tasks": total_pending,
            "workload_summary": {
                "pending": total_pending,
                "cluster_count": len(clusters),
                "capacity_total": capacity_total,
                "capacity_used_estimate": capacity_used_estimate,
                "capabilities": capability_workload,
            },
            "recent_routing_decisions": list(self._routing_decisions),
        }
