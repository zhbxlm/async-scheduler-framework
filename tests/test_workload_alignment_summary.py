from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_resource_manager_workload_summary_exposes_queue_depth_and_running_counts() -> None:
    from async_scheduler.platform.resource_manager import ResourceManager, ResourcePolicy

    @dataclass
    class FakeStats:
        pending: int
        running: int
        scheduled: int

    qm = MagicMock()
    qm.discover_capabilities = AsyncMock(return_value=["gpu", "cpu"])
    async def _stats(capability: str):
        if capability == "gpu":
            return FakeStats(pending=12, running=3, scheduled=2)
        else:
            return FakeStats(pending=0, running=0, scheduled=0)

    qm.get_capability_stats = AsyncMock(side_effect=_stats)

    rm = ResourceManager(queue_manager=qm)
    rm.register_policy("gpu", ResourcePolicy(scale_up_threshold=1.5, max_actors=4))
    rm.register_policy("cpu", ResourcePolicy(min_actors=0, scale_down_idle_seconds=0))

    # 先初始化缓存
    if not hasattr(rm, "_capability_stats"):
        rm._capability_stats = {}
    rm._capability_stats["gpu"] = FakeStats(pending=12, running=3, scheduled=2)
    rm._capability_stats["cpu"] = FakeStats(pending=0, running=0, scheduled=0)

    stats = rm.get_stats()

    # 确认 workload summary 存在并包含预期字段
    assert "workload_summary" in stats
    wl = stats["workload_summary"]
    assert "pending" in wl
    assert "running" in wl
    assert "scheduled" in wl
    assert "capabilities" in wl
    # gpu 的 pending 和 running 应计入
    assert wl["pending"] == 12
    assert wl["running"] == 3
    assert wl["scheduled"] == 2
    assert "gpu" in wl["capabilities"]
    assert wl["capabilities"]["gpu"]["pending"] == 12

    # 同时确认 scale_events 摘要也包含
    assert "recent_events" in stats
    assert stats["summary"]["scale_events_total"] >= 0
    assert stats["summary"]["tracked_capability_count"] == 2