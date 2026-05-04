from __future__ import annotations

import importlib

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_cluster_stats_endpoint_exposes_workload_summary() -> None:
    mod = importlib.import_module("async_scheduler.api.app")
    from async_scheduler.platform.cluster_registry import ClusterRegistry, ClusterInfo, ClusterHealth

    registry = ClusterRegistry()
    await registry.register_cluster(ClusterInfo(
        cluster_id="c1",
        name="GPU",
        capabilities=["gpu"],
        region="us-west",
        health=ClusterHealth.HEALTHY,
        pending_tasks=6,
        max_tasks=12,
    ))

    original_services = mod.services
    fake_services = type("FakeServices", (), {"cluster_registry": registry})()
    mod.services = fake_services
    try:
        async with AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.get("/clusters/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "workload_summary" in data
        assert data["workload_summary"]["pending"] == 6
        assert data["workload_summary"]["capacity_total"] == 12
    finally:
        mod.services = original_services
