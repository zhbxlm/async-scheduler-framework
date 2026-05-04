from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_consumer_publishes_running_event() -> None:
    from async_scheduler.core.consumer import TaskConsumer
    from async_scheduler.core.models import Task, TaskStatus
    from async_scheduler.platform.async_proxy import AsyncProxySidecar

    qm = MagicMock()
    executor = MagicMock()
    handler = AsyncMock()
    sidecar = AsyncProxySidecar()
    sidecar.publish = AsyncMock()

    consumer = TaskConsumer(queue_manager=qm, executor=executor, handler=handler, async_proxy_sidecar=sidecar)

    task = Task(name="demo", payload={}, priority=0, status=TaskStatus.QUEUED)
    await consumer._publish_running_event(task, attempt_id="attempt-1")

    sidecar.publish.assert_awaited_once()
    event = sidecar.publish.await_args.args[0]
    assert event.status == "running"
    assert event.completion_kind == "running"
    assert event.attempt_id == "attempt-1"
