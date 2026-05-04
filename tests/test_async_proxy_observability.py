from __future__ import annotations

import importlib

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_async_proxy_stats_include_summary_counters() -> None:
    from async_scheduler.platform.async_proxy import AsyncProxySidecar, TaskEvent

    sidecar = AsyncProxySidecar()
    await sidecar.start()
    await sidecar.publish(TaskEvent(task_id="t1", status="success"))
    await sidecar.publish(TaskEvent(task_id="t2", status="failed", error="boom"))
    await sidecar.wait_for("t1", timeout=0.01)
    await sidecar.wait_for("missing", timeout=0.01)

    stats = sidecar.get_stats()
    assert stats["published_total"] == 2
    assert stats["wait_requests_total"] == 2
    assert stats["wait_timeouts_total"] == 1
    assert stats["status_counts"]["success"] == 1
    assert stats["status_counts"]["failed"] == 1
    assert len(stats["recent_events"]) >= 2

    await sidecar.stop()


@pytest.mark.asyncio
async def test_async_proxy_stats_endpoint_returns_enhanced_shape() -> None:
    mod = importlib.import_module("async_scheduler.api.app")
    from async_scheduler.platform.async_proxy import AsyncProxySidecar, TaskEvent

    sidecar = AsyncProxySidecar()
    await sidecar.start()
    await sidecar.publish(TaskEvent(task_id="evt-1", status="success"))

    original_services = mod.services
    fake_services = type("FakeServices", (), {"async_proxy_sidecar": sidecar})()
    mod.services = fake_services
    try:
        async with AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.get("/async-proxy/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "published_total" in data
        assert "status_counts" in data
        assert "recent_events" in data
    finally:
        mod.services = original_services
        await sidecar.stop()
