from __future__ import annotations

import importlib

import pytest
from httpx import ASGITransport, AsyncClient


class _FakeAsyncProxy:
    def get_stats(self):
        return {
            "running": True,
            "published_total": 8,
            "status_counts": {"running": 2, "success": 4, "failed": 2},
            "recent_events": [],
        }


class _FakeDispatcher:
    async def get_stats(self):
        return {
            "redis_backed": True,
            "retry_queue_size": 2,
            "dead_letter_size": 1,
            "max_inline_attempts": 3,
            "max_persistent_attempts": 10,
            "recent_retry_events": [
                {"task_id": "task-r1", "attempt": 4, "next_retry_at": 12345.0, "callback_url": "https://retry.example.com"}
            ],
            "recent_dead_letters": [
                {"task_id": "task-d1", "attempt": 10, "callback_url": "https://dlq.example.com", "event_id": "evt-d1"}
            ],
        }


@pytest.mark.asyncio
async def test_async_proxy_stats_exposes_recent_callback_retry_and_dlq_summary() -> None:
    mod = importlib.import_module("async_scheduler.api.app")

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
        ccp = data["callback_control_plane"]
        assert len(ccp["recent_retry_events"]) == 1
        assert ccp["recent_retry_events"][0]["task_id"] == "task-r1"
        assert len(ccp["recent_dead_letters"]) == 1
        assert ccp["recent_dead_letters"][0]["task_id"] == "task-d1"
    finally:
        mod.services = original_services
