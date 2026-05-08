from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models.callback_outbox import CallbackDeliveryStatus
from src.services.callback_dispatcher import CallbackDispatchService


@pytest.mark.asyncio
async def test_callback_dispatcher_marks_delivered():
    row = SimpleNamespace(
        callback_url="http://cb",
        payload_json=json.dumps({"task_id": "t1"}),
        delivery_status=CallbackDeliveryStatus.PENDING,
        attempt_count=0,
        next_attempt_at=None,
        last_error=None,
        task_id="t1",
    )
    session = AsyncMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = [row]
    session.execute = AsyncMock(return_value=result)
    session.commit = AsyncMock()

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False
    def db_factory():
        return Factory()

    svc = CallbackDispatchService(db_factory)
    svc._client = AsyncMock()
    resp = AsyncMock()
    resp.raise_for_status = MagicMock()
    svc._client.post = AsyncMock(return_value=resp)

    processed = await svc.process_once()
    assert processed == 1
    assert row.delivery_status == CallbackDeliveryStatus.DELIVERED
    assert row.attempt_count == 1
