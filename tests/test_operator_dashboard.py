from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models.callback_outbox import CallbackDeliveryStatus
from src.services.operator_dashboard import OperatorDashboardService


@pytest.mark.asyncio
async def test_dashboard_summary_aggregates_sections():
    session = AsyncMock()
    cb_result = MagicMock()
    cb_result.fetchall.return_value = [(CallbackDeliveryStatus.PENDING, 2), (CallbackDeliveryStatus.DEAD_LETTER, 1)]
    action_result = MagicMock()
    action_result.scalar.return_value = 5
    event_result = MagicMock()
    event_result.scalar.return_value = 9
    session.execute = AsyncMock(side_effect=[cb_result, action_result, event_result])

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False
    def db_factory():
        return Factory()

    svc = OperatorDashboardService(db_factory)
    summary = await svc.summary()
    assert summary["callback"]["pending"] == 2
    assert summary["callback"]["dead_letter"] == 1
    assert summary["callback"]["total"] == 3
    assert summary["operator_actions"] == 5
    assert summary["recent_timeline_events"] == 9
