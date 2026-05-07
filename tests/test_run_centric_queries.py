from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.run_centric_queries import RunCentricQueryService


@pytest.mark.asyncio
async def test_list_task_runs_returns_ordered():
    rows = [SimpleNamespace(id=1, run_key="rk1", status="completed")]
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

    svc = RunCentricQueryService(db_factory)
    out = await svc.list_task_runs("t1")
    assert len(out) == 1
    assert out[0].run_key == "rk1"


@pytest.mark.asyncio
async def test_list_dag_runs_returns_ordered():
    rows = [SimpleNamespace(id=1, run_key="rk1", status="completed")]
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

    svc = RunCentricQueryService(db_factory)
    out = await svc.list_dag_runs("dag1")
    assert len(out) == 1
    assert out[0].run_key == "rk1"
