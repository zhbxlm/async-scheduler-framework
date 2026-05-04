from __future__ import annotations

import importlib
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_quota_stats_returns_usage_limits_and_summary() -> None:
    mod = importlib.import_module("src.main")

    mock_quota = MagicMock()
    mock_quota.stats = AsyncMock(return_value={
        "tenant-a": {
            "task_count": 12,
            "running_count": 3,
            "gpu_count": 1,
            "actor_count": 0,
            "max_queued": 50,
            "max_running": 10,
            "max_gpu": 2,
            "max_actor": 4,
        }
    })

    mock_services = MagicMock()
    mock_services.quota_manager = mock_quota

    original_services = mod.services
    mod.services = mock_services
    try:
        async with AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.get("/quota/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "tenants" in data
        assert "summary" in data
        assert data["summary"]["tenant_count"] == 1
        tenant = data["tenants"]["tenant-a"]
        assert tenant["queued"] == 12
        assert tenant["running"] == 3
        assert tenant["gpu"] == 1
        assert tenant["actors"] == 0
        assert tenant["max_queued"] == 50
        assert tenant["max_running"] == 10
        assert tenant["max_gpu"] == 2
        assert tenant["max_actor"] == 4
    finally:
        mod.services = original_services
