from __future__ import annotations

import importlib

import pytest
from httpx import ASGITransport, AsyncClient


class _FakeResourceManager:
    def get_stats(self):
        return {
            "running": True,
            "tracked_capabilities": ["gpu", "cpu"],
            "scale_events_total": 4,
            "recent_events": [],
            "workload_summary": {
                "pending": 15,
                "running": 3,
                "scheduled": 2,
                "capabilities": {
                    "gpu": {"pending": 12, "running": 2, "scheduled": 1},
                    "cpu": {"pending": 3, "running": 1, "scheduled": 1},
                },
            },
            "summary": {
                "scale_events_total": 4,
                "direction_counts": {"up": 2, "down": 1, "none": 1},
                "capability_counts": {"gpu": 2, "cpu": 2},
                "tracked_capability_count": 2,
            },
        }


@pytest.mark.asyncio
async def test_resources_stats_endpoint_exposes_workload_summary() -> None:
    mod = importlib.import_module("async_scheduler.api.app")

    fake_services = type("FakeServices", (), {"resource_manager": _FakeResourceManager()})()
    original_services = mod.services
    mod.services = fake_services
    try:
        async with AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.get("/resources/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "workload_summary" in data
        wl = data["workload_summary"]
        assert wl["pending"] == 15
        assert "gpu" in wl["capabilities"]
    finally:
        mod.services = original_services