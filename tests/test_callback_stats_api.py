from __future__ import annotations

import importlib

import pytest
from httpx import ASGITransport, AsyncClient


class _FakeDispatcher:
    async def get_stats(self):
        return {
            "redis_backed": True,
            "retry_queue_size": 3,
            "dead_letter_size": 1,
            "max_inline_attempts": 3,
            "max_persistent_attempts": 10,
        }


@pytest.mark.asyncio
async def test_callback_stats_endpoint_returns_summary() -> None:
    mod = importlib.import_module("async_scheduler.api.app")

    fake_services = type("FakeServices", (), {"callback_dispatcher": _FakeDispatcher()})()
    original_services = mod.services
    mod.services = fake_services
    try:
        async with AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.get("/callbacks/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["retry_queue_size"] == 3
        assert data["dead_letter_size"] == 1
    finally:
        mod.services = original_services
