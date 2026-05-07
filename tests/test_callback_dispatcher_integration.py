"""Tests for CallbackDispatcher control-plane integration."""
from __future__ import annotations

import pytest

from src.services.callback_dispatcher import CallbackDispatchService, _next_attempt_delay
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


def test_backoff_delay_increases_exponentially():
    """Verify backoff delay grows with each attempt."""
    delays = [_next_attempt_delay(i) for i in range(1, 9)]
    # Each delay should be >= previous
    for i in range(1, len(delays)):
        assert delays[i] >= delays[i - 1], f"delay[{i}]={delays[i]} < delay[{i-1}]={delays[i-1]}"
    # First attempt: 10s, last attempt: capped at 3600s
    assert delays[0] == 10
    assert delays[-1] == 3600


@pytest.mark.asyncio
async def test_failed_callback_gets_backoff_delay(async_db_session_factory):
    """After failure, next_attempt_at should be in the future, not immediate."""
    from datetime import datetime, timezone
    from sqlalchemy import select
    import httpx

    async with async_db_session_factory() as session:
        from src.models.callback_outbox import CallbackOutboxRecord, CallbackDeliveryStatus
        row = CallbackOutboxRecord(
            task_id="cb-backoff-t1",
            callback_url="http://localhost:29999/nonexistent",
            payload_json="{}",
            delivery_status=CallbackDeliveryStatus.PENDING,
        )
        session.add(row)
        await session.commit()

    svc = CallbackDispatchService(async_db_session_factory, max_attempts=5, poll_interval=1.0)
    svc._running = True
    svc._client = httpx.AsyncClient(timeout=1.0)
    await svc.process_once()
    await svc._client.aclose()

    now = datetime.now(timezone.utc)
    async with async_db_session_factory() as session:
        stmt = select(CallbackOutboxRecord).where(CallbackOutboxRecord.task_id == "cb-backoff-t1")
        result = await session.execute(stmt)
        updated = result.scalar_one()

    # Should have a future retry time (at least a few seconds out)
    assert updated.next_attempt_at is not None
    next_at = updated.next_attempt_at.replace(tzinfo=timezone.utc) if updated.next_attempt_at.tzinfo is None else updated.next_attempt_at
    assert next_at > now, "next_attempt_at should be in the future for backoff"
    assert updated.delivery_status.value == "pending"  # not dead_letter yet
