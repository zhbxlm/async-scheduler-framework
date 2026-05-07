from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.fake_redis import FullFakeAsyncRedis
from src.services.recovery_explainer import RecoveryExplainerService


@pytest.mark.asyncio
async def test_explain_task_running_without_lock_recommends_replay_or_repair():
    redis = FullFakeAsyncRedis()

    task = SimpleNamespace(task_id="t1", status="running", attempt=1, max_retries=3)
    session = AsyncMock()
    session.get = AsyncMock(return_value=task)

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False
    def db_factory():
        return Factory()

    svc = RecoveryExplainerService(redis_client=redis, session_factory=db_factory)
    result = await svc.explain_task("t1")
    assert result["lock"]["present"] is False
    assert result["mysql"]["status"] == "running"
    assert result["recommended_action"] == "replay_or_repair"


@pytest.mark.asyncio
async def test_explain_task_completed_recommends_callback_inspection():
    redis = FullFakeAsyncRedis()

    task = SimpleNamespace(task_id="t2", status="completed", attempt=1, max_retries=3)
    session = AsyncMock()
    session.get = AsyncMock(return_value=task)

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False
    def db_factory():
        return Factory()

    svc = RecoveryExplainerService(redis_client=redis, session_factory=db_factory)
    result = await svc.explain_task("t2")
    assert result["recommended_action"] == "inspect_or_replay_callback"
