from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.operator_queries import OperatorQueryService


@pytest.mark.asyncio
async def test_list_actions_returns_rows():
    rows = [SimpleNamespace(id=1, actor="alice", action_type="task_replay_requested", target_type="task", target_id="t1")]
    session = AsyncMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = rows
    session.execute = AsyncMock(return_value=result)

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False
    def db_factory():
        return Factory()

    svc = OperatorQueryService(db_factory)
    out = await svc.list_actions(target_type="task")
    assert len(out) == 1
    assert out[0].actor == "alice"
