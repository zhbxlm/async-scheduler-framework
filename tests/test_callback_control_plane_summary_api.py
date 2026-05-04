from __future__ import annotations

import importlib

import pytest
from httpx import ASGITransport, AsyncClient


class _FakeCallbackDispatcher:
    async def get_stats(self):
        return {
            "redis_backed": True,
            "retry_queue_size": 3,
            "dead_letter_size": 2,
            "max_inline_attempts": 2,
            "max_persistent_attempts": 6,
            "recent_retry_events": [{"task_id": "t1"}],
            "recent_dead_letters": [{"task_id": "t2"}, {"task_id": "t3"}],
        }


@pytest.mark.asyncio
async def test_callback_stats_exposes_control_plane_summary() -> None:
    mod = importlib.import_module("src.main")

    fake_services = type("FakeServices", (), {"callback_dispatcher": _FakeCallbackDispatcher()})()
    original_services = mod.services
    mod.services = fake_services
    try:
        async with AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.get("/callbacks/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "control_plane_summary" in data
        summary = data["control_plane_summary"]
        assert summary["queue_depth_total"] == 5
        assert summary["retry_queue_size"] == 3
        assert summary["dead_letter_size"] == 2
        assert summary["recent_retry_count"] == 1
        assert summary["recent_dead_letter_count"] == 2
    finally:
        mod.services = original_services
