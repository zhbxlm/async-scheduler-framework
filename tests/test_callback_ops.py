from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models.callback_outbox import CallbackDeliveryStatus
from src.services.callback_ops import CallbackOpsService


@pytest.mark.asyncio
async def test_list_dead_letters_returns_rows():
    row = SimpleNamespace(id=1, delivery_status=CallbackDeliveryStatus.DEAD_LETTER)
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

    svc = CallbackOpsService(db_factory)
    rows = await svc.list_dead_letters()
    assert len(rows) == 1
    assert rows[0].id == 1


@pytest.mark.asyncio
async def test_replay_dead_letter_resets_row():
    row = SimpleNamespace(id=1, delivery_status=CallbackDeliveryStatus.DEAD_LETTER, next_attempt_at="x", last_error="boom")
    session = AsyncMock()
    session.add = MagicMock()
    session.get = AsyncMock(return_value=row)
    session.commit = AsyncMock()

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False
    def db_factory():
        return Factory()

    svc = CallbackOpsService(db_factory)
    ok = await svc.replay_dead_letter(1)
    assert ok is True
    assert row.delivery_status == CallbackDeliveryStatus.PENDING
    assert row.next_attempt_at is None
    assert row.last_error is None
