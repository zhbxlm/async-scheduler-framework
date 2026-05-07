from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.operator_ux import OperatorUXService


@pytest.mark.asyncio
async def test_recent_operator_actions_returns_list():
    from src.models.operator_action import OperatorActionRecord
    from datetime import datetime

    row = SimpleNamespace(id=1, actor="alice", action_type="task_replay_requested", target_type="task", target_id="t1", reason="test", created_at=datetime(2026,1,1))
    session = AsyncMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = [row]
    session.execute = AsyncMock(return_value=result)

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False
    def db_factory():
        return Factory()

    svc = OperatorUXService(db_factory)
    items = await svc.recent_operator_actions()
    assert len(items) == 1
    assert items[0]["actor"] == "alice"


@pytest.mark.asyncio
async def test_stale_task_queue_returns_list():
    from datetime import datetime

    row = SimpleNamespace(task_id="t1", run_key="rk1", status="running", created_at=datetime(2026,1,1))
    session = AsyncMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = [row]
    session.execute = AsyncMock(return_value=result)

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False
    def db_factory():
        return Factory()

    svc = OperatorUXService(db_factory)
    items = await svc.stale_task_queue()
    assert len(items) == 1
    assert items[0]["task_id"] == "t1"
