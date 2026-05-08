from __future__ import annotations

import pytest

from src.services.replay_policy import ReplayPolicyService
from tests.fake_redis import FullFakeAsyncRedis


@pytest.mark.asyncio
async def test_replay_requires_reason():
    svc = ReplayPolicyService(redis_client=None)
    result = await svc.check_task_replay_allowed("t1", reason=None)
    assert result["allowed"] is False
    assert "replay reason is required" in result["reasons"]


@pytest.mark.asyncio
async def test_replay_denied_with_active_lease():
    redis = FullFakeAsyncRedis()
    await redis.set("task_lock:t1", "worker-abc")

    svc = ReplayPolicyService(redis_client=redis)
    result = await svc.check_task_replay_allowed("t1", reason="manual retry")
    assert result["allowed"] is False
    assert "active execution lease exists" in result["reasons"]


@pytest.mark.asyncio
async def test_replay_allowed_with_reason_and_no_lock():
    svc = ReplayPolicyService(redis_client=None)
    result = await svc.check_task_replay_allowed("t1", reason="manual retry")
    assert result["allowed"] is True
    assert result["reasons"] == ["allowed"]
