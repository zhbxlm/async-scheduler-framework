"""Tests for CallbackDispatcher control-plane integration."""
from __future__ import annotations

import pytest

from src.services.callback_dispatcher import CallbackDispatchService
from src.common.lifecycle import CallbackDispatcherResource


@pytest.mark.asyncio
async def test_callback_dispatcher_resource_start_stop():
    """CallbackDispatcherResource correctly starts and stops the service."""
    svc = CallbackDispatchService(None)
    resource = CallbackDispatcherResource(svc)
    assert resource.name == "CallbackDispatcher"

    await resource.start()
    assert svc._running is True
    assert svc._task is not None

    await resource.stop()
    assert svc._running is False


@pytest.mark.asyncio
async def test_callback_dispatcher_process_once_no_db():
    """process_once returns 0 when no DB is configured."""
    svc = CallbackDispatchService(None)
    svc._running = True
    import httpx
    svc._client = httpx.AsyncClient()
    count = await svc.process_once()
    assert count == 0
    await svc._client.aclose()


@pytest.mark.asyncio
async def test_callback_dispatcher_integration_with_real_db(async_db_session_factory):
    """CallbackDispatchService.process_once queries DB for pending callbacks."""
    from src.models.callback_outbox import CallbackOutboxRecord, CallbackDeliveryStatus
    import httpx

    # Write a pending callback
    async with async_db_session_factory() as session:
        row = CallbackOutboxRecord(
            task_id="cb-dispatch-t1",
            callback_url="http://localhost:19999/nonexistent",
            payload_json='{"result": "ok"}',
            delivery_status=CallbackDeliveryStatus.PENDING,
        )
        session.add(row)
        await session.commit()

    svc = CallbackDispatchService(async_db_session_factory, max_attempts=1)
    svc._running = True
    svc._client = httpx.AsyncClient(timeout=1.0)
    # This will fail (no server at 19999) and move to DEAD_LETTER after 1 attempt
    count = await svc.process_once()
    assert count == 1

    async with async_db_session_factory() as session:
        from sqlalchemy import select
        stmt = select(CallbackOutboxRecord).where(CallbackOutboxRecord.task_id == "cb-dispatch-t1")
        result = await session.execute(stmt)
        updated = result.scalar_one()
    # After 1 failed attempt with max_attempts=1, should be DEAD_LETTER
    assert updated.delivery_status == CallbackDeliveryStatus.DEAD_LETTER

    await svc._client.aclose()
