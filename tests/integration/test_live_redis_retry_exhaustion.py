from __future__ import annotations

import os

import pytest

redis_asyncio = pytest.importorskip("redis.asyncio")

from async_scheduler.backends.factory import BackendConfig, BackendFactory  # noqa: E402
from async_scheduler.core.consumer import TaskConsumer  # noqa: E402
from async_scheduler.core.models import ExecutionAttemptStatus, Task, TaskCreate, TaskStatus  # noqa: E402
from async_scheduler.executor import TaskExecutor  # noqa: E402
from async_scheduler.persistence import (  # noqa: E402
    ExecutionAttemptRepository,
    TaskRepository,
    drop_db,
    get_session,
    get_session_no_context,
    init_db,
)
from async_scheduler.queue import QueueManager  # noqa: E402


LIVE_REDIS_URL = os.getenv("TEST_REDIS_URL")
pytestmark = pytest.mark.skipif(not LIVE_REDIS_URL, reason="TEST_REDIS_URL not set")


@pytest.mark.asyncio
async def test_live_redis_consumer_failure_after_retry_budget_exhaustion_converges_to_failed() -> None:
    await drop_db()
    await init_db()

    factory = BackendFactory(
        BackendConfig(
            queue_type="redis",
            lock_type="redis",
            registry_type="memory",
            redis_url=LIVE_REDIS_URL,
            distributed_mode=True,
            lease_ttl_seconds=1.0,
            heartbeat_interval_seconds=0.05,
        )
    )
    queue_manager = QueueManager(factory.create_queue_backend())
    lock_backend = factory.create_lock_backend()

    async with get_session() as session:
        task = await TaskRepository.create(
            session,
            TaskCreate(name="live-retry-exhaustion", payload={"value": 3}, max_retries=1),
        )
        queued = Task.model_validate(task.model_dump())
        queued.status = TaskStatus.QUEUED
        await TaskRepository.update(session, task.id, status=TaskStatus.QUEUED)
        await queue_manager.enqueue(queued)

    async def handler(payload):
        raise RuntimeError(f"boom-{payload['value']}")

    consumer = TaskConsumer(
        queue_manager=queue_manager,
        executor=TaskExecutor(),
        handler=handler,
        lock_backend=lock_backend,
        worker_id="worker-live-retry-exhaustion",
        lease_ttl_seconds=1.0,
        heartbeat_interval_seconds=0.05,
    )

    processed = await consumer._process_next_once()
    assert processed is True

    async with await get_session_no_context() as session:
        stored_task = await TaskRepository.get(session, task.id)
        latest_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

    assert stored_task is not None
    assert stored_task.status == TaskStatus.FAILED
    assert stored_task.error_message == "boom-3"
    assert stored_task.retry_count == 1

    assert latest_attempt is not None
    assert latest_attempt.status == ExecutionAttemptStatus.FAILED
    assert latest_attempt.error_message == "boom-3"
    assert latest_attempt.retry_index == 0

    assert await queue_manager.dequeue(timeout=0.01) is None
