from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.replay_policy import ReplayPolicyService


@pytest.mark.asyncio
async def test_replay_rate_limit_denied_when_exceeded():
    session = AsyncMock()
    result = MagicMock()
    result.scalar.return_value = 3  # already at max
    session.execute = AsyncMock(return_value=result)

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False
    def db_factory():
        return Factory()

    svc = ReplayPolicyService(session_factory=db_factory, max_replays=3, window_seconds=300)
    result = await svc.check_task_replay_allowed("t1", reason="retry")
    assert result["allowed"] is False
    assert any("rate limit" in r for r in result["reasons"])


@pytest.mark.asyncio
async def test_replay_rate_limit_allowed_under_limit():
    session = AsyncMock()
    count_result = MagicMock()
    count_result.scalar.return_value = 1  # under limit
    session.execute = AsyncMock(return_value=count_result)

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False
    def db_factory():
        return Factory()

    svc = ReplayPolicyService(session_factory=db_factory, max_replays=3, window_seconds=300)
    result = await svc.check_task_replay_allowed("t1", reason="retry")
    assert result["allowed"] is True
