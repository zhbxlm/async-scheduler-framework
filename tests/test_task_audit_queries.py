from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.task_audit_queries import TaskAuditQueryService
from src.models.callback_outbox import CallbackDeliveryStatus


@pytest.mark.asyncio
async def test_get_task_timeline_returns_ordered_rows():
    rows = [SimpleNamespace(task_id="t1", event_type="task_created"), SimpleNamespace(task_id="t1", event_type="task_completed")]
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

    svc = TaskAuditQueryService(db_factory)
    out = await svc.get_task_timeline("t1")
    assert len(out) == 2
    assert out[0].event_type == "task_created"


@pytest.mark.asyncio
async def test_get_callback_summary_aggregates_counts():
    session = AsyncMock()
    result = MagicMock()
    result.fetchall.return_value = [
        (CallbackDeliveryStatus.PENDING, 2),
        (CallbackDeliveryStatus.DEAD_LETTER, 1),
    ]
    session.execute = AsyncMock(return_value=result)

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False
    def db_factory():
        return Factory()

    svc = TaskAuditQueryService(db_factory)
    summary = await svc.get_callback_summary()
    assert summary["pending"] == 2
    assert summary["dead_letter"] == 1
    assert summary["total"] == 3
