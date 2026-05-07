from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.operator_actions import OperatorActionService


@pytest.mark.asyncio
async def test_operator_action_record_persists():
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

    svc = OperatorActionService(db_factory)
    await svc.record(actor="alice", action_type="callback_replay_requested", target_type="callback_outbox", target_id="1", task_id="t1")
    assert session.add.call_count >= 1
    session.commit.assert_awaited()
