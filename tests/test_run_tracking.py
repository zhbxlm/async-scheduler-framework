from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.run_tracking import RunTrackingService


@pytest.mark.asyncio
async def test_create_task_run_persists_record():
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

    svc = RunTrackingService(db_factory)
    run_key = await svc.create_task_run(task_id="t1", attempt=1, status="created")
    assert run_key is not None
    session.add.assert_called_once()
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_dag_run_persists_record():
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

    svc = RunTrackingService(db_factory)
    run_key = await svc.create_dag_run(dag_id="dag-a", tenant_id="t1")
    assert run_key is not None
    session.add.assert_called_once()
    session.commit.assert_awaited_once()
