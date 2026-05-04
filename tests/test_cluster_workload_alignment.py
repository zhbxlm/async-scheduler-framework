from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_cluster_registry_stats_include_workload_summary() -> None:
    from async_scheduler.platform.cluster_registry import ClusterRegistry, ClusterInfo, ClusterHealth

    registry = ClusterRegistry()
    await registry.register_cluster(ClusterInfo(
        cluster_id="c1",
        name="GPU-A",
        capabilities=["gpu", "ml"],
        health=ClusterHealth.HEALTHY,
        region="us-west",
        pending_tasks=7,
        max_tasks=10,
    ))
    await registry.register_cluster(ClusterInfo(
        cluster_id="c2",
        name="CPU-A",
        capabilities=["cpu"],
        health=ClusterHealth.DEGRADED,
        region="us-east",
        pending_tasks=2,
        max_tasks=8,
    ))

    stats = await registry.get_stats_async()
    assert "workload_summary" in stats
    wl = stats["workload_summary"]
    assert wl["pending"] == 9
    assert wl["cluster_count"] == 2
    assert wl["capacity_total"] == 18
    assert wl["capacity_used_estimate"] == 9
    assert wl["capabilities"]["gpu"]["pending"] == 7
    assert wl["capabilities"]["cpu"]["pending"] == 2


@pytest.mark.asyncio
async def test_cluster_registry_sync_stats_include_workload_summary() -> None:
    from async_scheduler.platform.cluster_registry import ClusterRegistry, ClusterInfo, ClusterHealth

    registry = ClusterRegistry()
    await registry.register_cluster(ClusterInfo(
        cluster_id="c1",
        name="GPU-A",
        capabilities=["gpu"],
        health=ClusterHealth.HEALTHY,
        region="us-west",
        pending_tasks=4,
        max_tasks=10,
    ))

    stats = registry.get_stats()
    assert "workload_summary" in stats
    assert stats["workload_summary"]["pending"] == 4
    assert stats["workload_summary"]["capabilities"]["gpu"]["cluster_count"] == 1
