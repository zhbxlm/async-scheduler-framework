from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime

import pytest

from src.services.replay_chain_queries import ReplayChainQueryService


@pytest.mark.asyncio
async def test_get_replay_chain_returns_ordered_runs():
    rows = [
        SimpleNamespace(run_key="rk1", status="completed", attempt=0, trigger_source=None, created_at=datetime(2026,1,1)),
        SimpleNamespace(run_key="rk2", status="replay_requested", attempt=1, trigger_source="operator", created_at=datetime(2026,1,2)),
    ]
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

    svc = ReplayChainQueryService(db_factory)
    chain = await svc.get_replay_chain("t1")
    assert len(chain) == 2
    assert chain[0]["run_key"] == "rk1"
    assert chain[1]["attempt"] == 1
