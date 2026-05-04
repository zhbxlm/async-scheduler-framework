from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_resource_manager_stats_include_direction_summary() -> None:
    from async_scheduler.platform.resource_manager import ResourceManager, ResourcePolicy

    @dataclass
    class FakeStats:
        pending: int
        running: int

    qm = MagicMock()
    qm.discover_capabilities = AsyncMock(return_value=["gpu", "cpu"])
    async def _stats(capability: str):
        if capability == "gpu":
            return FakeStats(pending=10, running=1)
        return FakeStats(pending=0, running=0)

    qm.get_capability_stats = AsyncMock(side_effect=_stats)

    rm = ResourceManager(queue_manager=qm)
    rm.register_policy("gpu", ResourcePolicy(scale_up_threshold=1.5, max_actors=4))
    rm.register_policy("cpu", ResourcePolicy(min_actors=0, scale_down_idle_seconds=0))

    await rm._check_all_capabilities()
    await rm._check_all_capabilities()

    stats = rm.get_stats()
    assert "summary" in stats
    assert "direction_counts" in stats["summary"]
    assert stats["summary"]["scale_events_total"] >= 1


@pytest.mark.asyncio
async def test_cluster_registry_stats_include_health_and_capability_summary() -> None:
    from async_scheduler.platform.cluster_registry import ClusterRegistry, ClusterInfo, ClusterHealth, RoutingPolicy

    registry = ClusterRegistry()
    await registry.register_cluster(ClusterInfo(
        cluster_id="c1", name="GPU", capabilities=["gpu", "ml"],
        health=ClusterHealth.HEALTHY, region="us-west", pending_tasks=5, max_tasks=10,
    ))
    await registry.register_cluster(ClusterInfo(
        cluster_id="c2", name="CPU", capabilities=["cpu"],
        health=ClusterHealth.DEGRADED, region="us-east", pending_tasks=1, max_tasks=10,
    ))

    await registry.route_task(RoutingPolicy(required_capabilities=["gpu"]))
    stats = await registry.get_stats_async()

    assert stats["cluster_count"] == 2
    assert stats["health_counts"]["healthy"] == 1
    assert stats["health_counts"]["degraded"] == 1
    assert stats["capability_counts"]["gpu"] == 1
    assert stats["capability_counts"]["cpu"] == 1
    assert len(stats["recent_routing_decisions"]) >= 1
