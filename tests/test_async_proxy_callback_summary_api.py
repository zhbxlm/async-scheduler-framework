from __future__ import annotations

import importlib

import pytest
from httpx import ASGITransport, AsyncClient


class _FakeAsyncProxy:
    def get_stats(self):
        return {
            "running": True,
            "published_total": 5,
            "status_counts": {"running": 1, "success": 2, "failed": 2},
            "recent_events": [],
        }


class _FakeDispatcher:
    async def get_stats(self):
        return {
            "redis_backed": True,
            "retry_queue_size": 4,
            "dead_letter_size": 1,
            "max_inline_attempts": 3,
            "max_persistent_attempts": 10,
        }


@pytest.mark.asyncio
async def test_async_proxy_stats_includes_callback_control_plane_summary() -> None:
    mod = importlib.import_module("src.main")

    fake_services = type(
        "FakeServices",
        (),
        {"async_proxy_sidecar": _FakeAsyncProxy(), "callback_dispatcher": _FakeDispatcher()},
    )()
    original_services = mod.services
    mod.services = fake_services
    try:
        async with AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.get("/async-proxy/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "callback_control_plane" in data
        assert data["callback_control_plane"]["retry_queue_size"] == 4
        assert data["callback_control_plane"]["dead_letter_size"] == 1
    finally:
        mod.services = original_services
