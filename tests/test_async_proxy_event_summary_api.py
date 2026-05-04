from __future__ import annotations

import importlib

import pytest
from httpx import ASGITransport, AsyncClient


class _FakeAsyncProxy:
    def get_stats(self):
        return {
            "running": True,
            "redis_connected": True,
            "cached_events": 2,
            "active_waiters": 1,
            "subscriber_count": 1,
            "published_total": 8,
            "wait_requests_total": 5,
            "wait_timeouts_total": 1,
            "subscriber_errors_total": 0,
            "redis_publish_failures_total": 0,
            "status_counts": {"running": 2, "success": 3, "failed": 2, "callback_requeued": 1},
            "recent_events": [],
        }


class _FakeCallbackDispatcher:
    async def get_stats(self):
        return {
            "redis_backed": True,
            "retry_queue_size": 4,
            "dead_letter_size": 1,
            "max_inline_attempts": 2,
            "max_persistent_attempts": 5,
            "recent_retry_events": [],
            "recent_dead_letters": [],
        }


@pytest.mark.asyncio
async def test_async_proxy_stats_exposes_event_summary() -> None:
    mod = importlib.import_module("src.main")

    fake_services = type(
        "FakeServices",
        (),
        {"async_proxy_sidecar": _FakeAsyncProxy(), "callback_dispatcher": _FakeCallbackDispatcher()},
    )()
    original_services = mod.services
    mod.services = fake_services
    try:
        async with AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.get("/async-proxy/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "event_summary" in data
        summary = data["event_summary"]
        assert summary["published_total"] == 8
        assert summary["terminal_total"] == 5
        assert summary["callback_event_total"] == 1
        assert summary["running_total"] == 2
        assert summary["callback_control_plane"]["retry_queue_size"] == 4
    finally:
        mod.services = original_services
