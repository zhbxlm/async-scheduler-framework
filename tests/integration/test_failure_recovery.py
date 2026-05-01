from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

from async_scheduler.backends.factory import BackendConfig, BackendFactory
from async_scheduler.core.consumer import TaskConsumer
from async_scheduler.core.models import ExecutionAttemptCreate, ExecutionAttemptStatus, Task, TaskCreate, TaskStatus
from async_scheduler.distributed.worker_registry import WorkerInfo, WorkerRegistry
from async_scheduler.executor import TaskExecutor
from async_scheduler.persistence import (
    ExecutionAttemptRepository,
    TaskRepository,
    drop_db,
    get_session,
    get_session_no_context,
    init_db,
)
from async_scheduler.platform.reconciler import ReconciliationConfig, RepairStrategy, TaskReconciler
from async_scheduler.queue import QueueManager


@pytest.mark.asyncio
async def test_dead_worker_task_is_recovered_by_reconciler() -> None:
    await drop_db()
    await init_db()

    factory = BackendFactory(
        BackendConfig(
            queue_type="redis",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
            distributed_mode=True,
            lease_ttl_seconds=0.1,
            heartbeat_interval_seconds=0.05,
        )
    )
    queue_manager = QueueManager(factory.create_queue_backend())
    lock_backend = factory.create_lock_backend()
    worker_registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=0.1)
    reconciler = TaskReconciler(
        config=ReconciliationConfig(stuck_after_seconds=0, repair_strategy=RepairStrategy.REQUEUE),
        queue_manager=queue_manager,
        lock_backend=lock_backend,
        worker_registry=worker_registry,
    )

    async with get_session() as session:
        task = await TaskRepository.create(session, TaskCreate(name="recover-me", payload={}))
        await TaskRepository.update(
            session,
            task.id,
            status=TaskStatus.RUNNING,
            started_at=datetime.utcnow() - timedelta(seconds=10),
            updated_at=datetime.utcnow() - timedelta(seconds=10),
        )
        attempt = await ExecutionAttemptRepository.create(
            session,
            ExecutionAttemptCreate(
                task_id=task.id,
                worker_id="worker-dead",
                retry_index=0,
                lease_token="lease-dead",
            ),
        )
        await ExecutionAttemptRepository.update(
            session,
            attempt.id,
            status=ExecutionAttemptStatus.RUNNING,
            started_at=datetime.utcnow() - timedelta(seconds=10),
            last_heartbeat_at=datetime.utcnow() - timedelta(seconds=10),
        )

    await worker_registry.register(WorkerInfo(worker_id="worker-dead", name="dead"))
    handle = await lock_backend.acquire(f"task:{task.id}", ttl=0.05)
    assert handle is not None
    await asyncio.sleep(0.12)

    repaired = await reconciler.reconcile()

    async with await get_session_no_context() as session:
        updated_task = await TaskRepository.get(session, task.id)
        updated_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

    assert repaired == 1
    assert updated_task is not None and updated_task.status == TaskStatus.QUEUED
    assert updated_attempt is not None and updated_attempt.status == ExecutionAttemptStatus.ABANDONED


@pytest.mark.asyncio
async def test_lease_loss_during_execution_causes_failure_then_recovery() -> None:
    await drop_db()
    await init_db()

    factory = BackendFactory(
        BackendConfig(
            queue_type="redis",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
            distributed_mode=True,
            lease_ttl_seconds=0.08,
            heartbeat_interval_seconds=0.05,
        )
    )
    queue_manager = QueueManager(factory.create_queue_backend())
    lock_backend = factory.create_lock_backend()
    worker_registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=0.08)
    reconciler = TaskReconciler(
        config=ReconciliationConfig(stuck_after_seconds=0, repair_strategy=RepairStrategy.REQUEUE),
        queue_manager=queue_manager,
        lock_backend=lock_backend,
        worker_registry=worker_registry,
    )

    async with get_session() as session:
        task = await TaskRepository.create(session, TaskCreate(name="lease-loss", payload={}))
        queued = Task.model_validate(task.model_dump())
        queued.status = TaskStatus.QUEUED
        await TaskRepository.update(session, task.id, status=TaskStatus.QUEUED)
        await queue_manager.enqueue(queued)

    await worker_registry.register(WorkerInfo(worker_id="worker-lease", name="lease"))

    async def handler(payload):
        await asyncio.sleep(0.2)
        return {"done": True}

    consumer = TaskConsumer(
        queue_manager=queue_manager,
        executor=TaskExecutor(),
        handler=handler,
        lock_backend=lock_backend,
        worker_id="worker-lease",
        lease_ttl_seconds=0.08,
        heartbeat_interval_seconds=0.2,
    )

    await consumer._process_next_once()
        
    async with await get_session_no_context() as session:
        latest_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)
        current_task = await TaskRepository.get(session, task.id)

    assert latest_attempt is not None
    assert latest_attempt.status == ExecutionAttemptStatus.ABANDONED
    assert current_task is not None
    assert current_task.status == TaskStatus.FAILED

    await worker_registry.deregister("worker-lease")
    repaired = await reconciler.reconcile()

    async with await get_session_no_context() as session:
        recovered_task = await TaskRepository.get(session, task.id)

    assert repaired == 0
    assert recovered_task is not None
    assert recovered_task.status == TaskStatus.FAILED
