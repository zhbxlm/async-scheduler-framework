from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.replay_lineage import ReplayLineageService


@pytest.mark.asyncio
async def test_replay_task_creates_new_run_and_returns_lineage():
    session = AsyncMock()
    session.add = MagicMock()
    session.commit = AsyncMock()

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False
    def db_factory():
        return Factory()

    svc = ReplayLineageService(db_factory)
    result = await svc.replay_task(task_id="t1", actor="alice", reason="manual retry", from_run_key="rk-old")
    assert result["ok"] is True
    assert result["task_id"] == "t1"
    assert result["new_run_key"] is not None
    assert result["from_run_key"] == "rk-old"
