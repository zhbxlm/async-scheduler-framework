from __future__ import annotations

import os

import pytest

redis_asyncio = pytest.importorskip("redis.asyncio")

from async_scheduler.backends.factory import BackendConfig, BackendFactory  # noqa: E402
from async_scheduler.core.consumer import TaskConsumer  # noqa: E402
from async_scheduler.core.models import ExecutionAttemptStatus, Task, TaskCreate, TaskStatus  # noqa: E402
from async_scheduler.distributed.worker_registry import WorkerInfo, WorkerRegistry  # noqa: E402
from async_scheduler.executor import TaskExecutor  # noqa: E402
from async_scheduler.persistence import (  # noqa: E402
    ExecutionAttemptRepository,
    TaskRepository,
    drop_db,
    get_session,
    get_session_no_context,
    init_db,
)
from async_scheduler.platform.reconciler import (  # noqa: E402
    ReconciliationConfig,
    RepairStrategy,
    TaskReconciler,
)
from async_scheduler.queue import QueueManager  # noqa: E402


LIVE_REDIS_URL = os.getenv("TEST_REDIS_URL")
pytestmark = pytest.mark.skipif(not LIVE_REDIS_URL, reason="TEST_REDIS_URL not set")


@pytest.mark.asyncio
async def test_live_redis_consumer_lease_loss_then_reconciler_requeues() -> None:
    await drop_db()
    await init_db()

    factory = BackendFactory(
        BackendConfig(
            queue_type="redis",
            lock_type="redis",
            registry_type="memory",
            redis_url=LIVE_REDIS_URL,
            distributed_mode=True,
            lease_ttl_seconds=0.08,
            heartbeat_interval_seconds=0.2,
        )
    )
    queue_manager = QueueManager(factory.create_queue_backend())
    lock_backend = factory.create_lock_backend()
    worker_registry = WorkerRegistry(redis_url=LIVE_REDIS_URL, heartbeat_ttl_seconds=0.08)
    reconciler = TaskReconciler(
        config=ReconciliationConfig(stuck_after_seconds=0, repair_strategy=RepairStrategy.REQUEUE),
        queue_manager=queue_manager,
        lock_backend=lock_backend,
        worker_registry=worker_registry,
    )

    async with get_session() as session:
        task = await TaskRepository.create(session, TaskCreate(name="live-consumer-recovery", payload={"value": 1}))
        queued = Task.model_validate(task.model_dump())
        queued.status = TaskStatus.QUEUED
        await TaskRepository.update(session, task.id, status=TaskStatus.QUEUED)
        await queue_manager.enqueue(queued)

    await worker_registry.register(WorkerInfo(worker_id="worker-live-consumer", name="consumer"))

    async def handler(payload):
        import asyncio

        await asyncio.sleep(0.2)
        return {"ok": payload["value"]}

    consumer = TaskConsumer(
        queue_manager=queue_manager,
        executor=TaskExecutor(),
        handler=handler,
        lock_backend=lock_backend,
        worker_id="worker-live-consumer",
        lease_ttl_seconds=0.08,
        heartbeat_interval_seconds=0.2,
    )

    processed = await consumer._process_next_once()
    assert processed is True

    async with await get_session_no_context() as session:
        failed_task = await TaskRepository.get(session, task.id)
        failed_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

    assert failed_task is not None
    assert failed_task.status == TaskStatus.FAILED
    assert failed_attempt is not None
    assert failed_attempt.status == ExecutionAttemptStatus.ABANDONED

    await worker_registry.deregister("worker-live-consumer")
    repaired = await reconciler.reconcile()

    async with await get_session_no_context() as session:
        recovered_task = await TaskRepository.get(session, task.id)
        recovered_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

    queued_again = await queue_manager.dequeue()

    assert repaired == 1
    assert recovered_task is not None
    assert recovered_task.status == TaskStatus.QUEUED
    assert recovered_attempt is not None
    assert recovered_attempt.status == ExecutionAttemptStatus.ABANDONED
    assert queued_again is not None
    assert queued_again.id == task.id
