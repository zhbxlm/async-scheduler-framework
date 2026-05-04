"""Test callback requeued and dead-letter events in Async Proxy sidecar."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock

import pytest

from async_scheduler.platform.async_proxy import AsyncProxySidecar, TaskEvent
from async_scheduler.platform.callback import CallbackDispatcher


@pytest.mark.asyncio
async def test_callback_dispatcher_publishes_requeued_event_when_callback_fails_and_goes_to_retry() -> None:
    """When a callback fails and is placed into retry queue, sidecar should receive a 'callback_requeued' event."""
    redis = AsyncMock()
    redis.zadd = AsyncMock()
    redis.zcard = AsyncMock(return_value=5)
    redis.zrangebyscore = AsyncMock(return_value=[])
    redis.zrem = AsyncMock(return_value=1)
    redis.set = AsyncMock()
    redis.get = AsyncMock(return_value=None)

    # Mock async proxy sidecar
    sidecar = AsyncProxySidecar(redis_client=redis)
    sidecar.publish = AsyncMock()

    dispatcher = CallbackDispatcher(
        redis_client=redis,
        max_inline_attempts=1,
        max_persistent_attempts=3,
        async_proxy_sidecar=sidecar,
    )

    # Mock callback dispatch to fail
    payload = {
        "task_id": "task-123",
        "name": "test-task",
        "status": "success",
        "result": {"value": 42},
        "completed_at": "2026-05-04T12:00:00Z",
    }
    dispatcher._send = AsyncMock(return_value=False)  # 模拟失败

    # Dispatch will fail and be requeued
    result = await dispatcher.dispatch("https://example.com/callback", payload)
    assert result is False  # L1 exhausted

    # Give async sidecar a moment
    await asyncio.sleep(0.05)

    # Verify sidecar.publish was called with a requeued event
    assert sidecar.publish.called
    args, kwargs = sidecar.publish.call_args
    event: TaskEvent = args[0]
    assert event.status == "callback_requeued"
    assert event.task_id == "task-123"
    assert event.callback_url == "https://example.com/callback"
    assert event.result["retry_queue_size"] == 5  # 来自 zcard mock

    await dispatcher.close()


@pytest.mark.asyncio
async def test_callback_dispatcher_publishes_dead_letter_event_when_retry_budget_exhausted() -> None:
    """When retry attempts are exhausted and callback moves to DLQ, sidecar should receive a 'callback_dead_letter' event."""
    redis = AsyncMock()
    redis.zadd = AsyncMock()
    redis.zcard = AsyncMock(return_value=0)
    redis.zrangebyscore = AsyncMock(return_value=['{"event_id": "evt1", "task_id": "task-dlq-1", "callback_url": "https://dlq.example.com", "payload": {}, "attempt": 3, "next_retry_at": 0}'])
    redis.zrem = AsyncMock(return_value=1)
    redis.set = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.keys = AsyncMock(return_value=["dlq:key1"])

    sidecar = AsyncProxySidecar(redis_client=redis)
    sidecar.publish = AsyncMock()

    dispatcher = CallbackDispatcher(
        redis_client=redis,
        max_inline_attempts=1,
        max_persistent_attempts=2,
        async_proxy_sidecar=sidecar,
    )

    # Mock _send to fail
    dispatcher._send = AsyncMock(return_value=False)

    # Process due callbacks (模拟 L2 重试处理)
    # 这会触发 DLQ 转移，因为 attempt >= max_persistent_attempts
    processed = await dispatcher.process_due_callbacks(batch_size=10)
    assert processed == 1

    # Verify sidecar.publish was called with dead_letter event
    assert sidecar.publish.called
    calls = sidecar.publish.call_args_list
    dead_letter_events = [c[0][0] for c in calls if c[0][0].status == "callback_dead_letter"]
    assert len(dead_letter_events) > 0
    event = dead_letter_events[0]
    assert event.task_id == "task-dlq-1"
    assert event.callback_url == "https://dlq.example.com"
    assert event.result["dead_letter_size"] == 1  # 来自 keys mock

    await dispatcher.close()