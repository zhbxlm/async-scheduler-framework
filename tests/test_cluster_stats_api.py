from __future__ import annotations

import importlib

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_cluster_stats_endpoint_returns_summary() -> None:
    mod = importlib.import_module("async_scheduler.api.app")
    from async_scheduler.platform.cluster_registry import ClusterRegistry, ClusterInfo

    registry = ClusterRegistry()
    await registry.register_cluster(ClusterInfo(cluster_id="c1", name="GPU", capabilities=["gpu"], region="us-west"))

    original_services = mod.services
    fake_services = type("FakeServices", (), {"cluster_registry": registry})()
    mod.services = fake_services
    try:
        async with AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.get("/clusters/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cluster_count"] == 1
        assert "capability_counts" in data
        assert "recent_routing_decisions" in data
    finally:
        mod.services = original_services
