from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.task_timeline import TaskTimelineService


@pytest.mark.asyncio
async def test_timeline_emit_persists_event():
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

    svc = TaskTimelineService(db_factory)
    await svc.emit(task_id="t1", event_type="task_created", payload={"k": "v"}, run_key="rk1")
    session.add.assert_called_once()
    session.commit.assert_awaited_once()
