from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

pytestmark = pytest.mark.mysql_required

import pytest_asyncio

from async_scheduler.backends.factory import BackendConfig, BackendFactory
from async_scheduler.core.models import ExecutionAttemptCreate, ExecutionAttemptStatus, TaskCreate, TaskStatus
from async_scheduler.distributed.worker_registry import WorkerInfo, WorkerRegistry
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
class TestDistributedReconciler:
    @pytest_asyncio.fixture(autouse=True)
    async def init_database(self):
        await drop_db()
        await init_db()
        yield

    async def test_stale_leased_task_gets_requeued(self) -> None:
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
            task = await TaskRepository.create(session, TaskCreate(name="stale-task", payload={}))
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
                    worker_id="worker-stale",
                    retry_index=0,
                    lease_token="lease-stale",
                ),
            )
            await ExecutionAttemptRepository.update(
                session,
                attempt.id,
                status=ExecutionAttemptStatus.RUNNING,
                started_at=datetime.utcnow() - timedelta(seconds=10),
                last_heartbeat_at=datetime.utcnow() - timedelta(seconds=10),
            )

        handle = await lock_backend.acquire(f"task:{task.id}", ttl=0.05)
        assert handle is not None
        await worker_registry.register(WorkerInfo(worker_id="worker-stale", name="stale"))
        await asyncio.sleep(0.12)

        repaired = await reconciler.reconcile()

        async with await get_session_no_context() as session:
            updated_task = await TaskRepository.get(session, task.id)
            updated_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

        assert repaired == 1
        assert updated_task is not None
        assert updated_task.status == TaskStatus.QUEUED
        assert updated_task.retry_count == 1
        assert updated_attempt is not None
        assert updated_attempt.status == ExecutionAttemptStatus.ABANDONED
        assert await queue_manager.get_queue_count() == 1

    async def test_active_leased_task_is_not_stolen(self) -> None:
        factory = BackendFactory(
            BackendConfig(
                queue_type="redis",
                lock_type="redis",
                registry_type="memory",
                redis_url="redis://localhost:6379/0",
                distributed_mode=True,
                lease_ttl_seconds=1.0,
                heartbeat_interval_seconds=0.05,
            )
        )
        queue_manager = QueueManager(factory.create_queue_backend())
        lock_backend = factory.create_lock_backend()
        worker_registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=1.0)
        reconciler = TaskReconciler(
            config=ReconciliationConfig(stuck_after_seconds=0, repair_strategy=RepairStrategy.REQUEUE),
            queue_manager=queue_manager,
            lock_backend=lock_backend,
            worker_registry=worker_registry,
        )

        async with get_session() as session:
            task = await TaskRepository.create(session, TaskCreate(name="active-task", payload={}))
            await TaskRepository.update(
                session,
                task.id,
                status=TaskStatus.RUNNING,
                started_at=datetime.utcnow() - timedelta(seconds=1),
                updated_at=datetime.utcnow() - timedelta(seconds=1),
            )
            attempt = await ExecutionAttemptRepository.create(
                session,
                ExecutionAttemptCreate(
                    task_id=task.id,
                    worker_id="worker-live",
                    retry_index=0,
                    lease_token="lease-live",
                ),
            )
            await ExecutionAttemptRepository.update(
                session,
                attempt.id,
                status=ExecutionAttemptStatus.RUNNING,
                started_at=datetime.utcnow() - timedelta(seconds=1),
                last_heartbeat_at=datetime.utcnow(),
            )

        handle = await lock_backend.acquire(f"task:{task.id}", ttl=1.0)
        assert handle is not None
        await worker_registry.register(WorkerInfo(worker_id="worker-live", name="live"))

        repaired = await reconciler.reconcile()

        async with await get_session_no_context() as session:
            updated_task = await TaskRepository.get(session, task.id)

        assert repaired == 0
        assert updated_task is not None
        assert updated_task.status == TaskStatus.RUNNING
        assert await queue_manager.get_queue_count() == 0

    async def test_two_reconcilers_do_not_double_repair_same_task(self) -> None:
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
        reconciler_a = TaskReconciler(
            config=ReconciliationConfig(stuck_after_seconds=0, repair_strategy=RepairStrategy.REQUEUE),
            queue_manager=queue_manager,
            lock_backend=lock_backend,
            worker_registry=worker_registry,
        )
        reconciler_b = TaskReconciler(
            config=ReconciliationConfig(stuck_after_seconds=0, repair_strategy=RepairStrategy.REQUEUE),
            queue_manager=queue_manager,
            lock_backend=lock_backend,
            worker_registry=worker_registry,
        )

        async with get_session() as session:
            task = await TaskRepository.create(session, TaskCreate(name="double-repair", payload={}))
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

        handle = await lock_backend.acquire(f"task:{task.id}", ttl=0.05)
        assert handle is not None
        await worker_registry.register(WorkerInfo(worker_id="worker-dead", name="dead"))
        await asyncio.sleep(0.12)

        repaired_counts = await asyncio.gather(reconciler_a.reconcile(), reconciler_b.reconcile())

        async with await get_session_no_context() as session:
            updated_task = await TaskRepository.get(session, task.id)

        assert sum(repaired_counts) == 1
        assert updated_task is not None
        assert updated_task.status == TaskStatus.QUEUED
        assert await queue_manager.get_queue_count() == 1
