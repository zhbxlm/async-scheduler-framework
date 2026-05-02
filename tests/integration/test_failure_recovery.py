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
from async_scheduler.platform.callback import CallbackDispatcher
from async_scheduler.platform.completion import TaskCompletionNode
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


class BlockingCallbackDispatcher(CallbackDispatcher):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.payloads: list[dict] = []

    async def dispatch(self, callback_url: str | None, payload: dict) -> bool:
        self.payloads.append(payload)
        self.started.set()
        await self.release.wait()
        return True


class FailingBlockingCallbackDispatcher(CallbackDispatcher):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.payloads: list[dict] = []
        self.calls = 0

    async def dispatch(self, callback_url: str | None, payload: dict) -> bool:
        self.calls += 1
        self.payloads.append(payload)
        self.started.set()
        await self.release.wait()
        raise RuntimeError("callback boom")


class ExtendFailOnceLockBackend:
    def __init__(self, inner) -> None:
        self._inner = inner
        self.extend_calls = 0

    async def acquire(self, key: str, ttl: float | None = None, wait: float | None = None):
        return await self._inner.acquire(key, ttl=ttl, wait=wait)

    async def release(self, handle):
        return await self._inner.release(handle)

    async def extend(self, handle, ttl: float):
        self.extend_calls += 1
        if self.extend_calls == 1:
            return False
        return await self._inner.extend(handle, ttl=ttl)

    async def is_locked(self, key: str):
        return await self._inner.is_locked(key)


@pytest.mark.asyncio
async def test_lease_loss_during_callback_dispatch_does_not_requeue_terminal_task() -> None:
    await drop_db()
    await init_db()

    factory = BackendFactory(
        BackendConfig(
            queue_type="redis",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
            distributed_mode=True,
            lease_ttl_seconds=0.05,
            heartbeat_interval_seconds=0.05,
        )
    )
    queue_manager = QueueManager(factory.create_queue_backend())
    lock_backend = factory.create_lock_backend()
    worker_registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=1.0)
    dispatcher = BlockingCallbackDispatcher()
    completion = TaskCompletionNode(callback_dispatcher=dispatcher)
    reconciler = TaskReconciler(
        config=ReconciliationConfig(stuck_after_seconds=0, repair_strategy=RepairStrategy.REQUEUE),
        queue_manager=queue_manager,
        lock_backend=lock_backend,
        worker_registry=worker_registry,
    )

    async with get_session() as session:
        task = await TaskRepository.create(
            session,
            TaskCreate(name="lease-loss-during-callback", payload={}, callback_url="https://callback/test"),
        )
        await TaskRepository.update(
            session,
            task.id,
            status=TaskStatus.RUNNING,
            started_at=datetime.utcnow() - timedelta(seconds=5),
            updated_at=datetime.utcnow() - timedelta(seconds=5),
        )
        attempt = await ExecutionAttemptRepository.create(
            session,
            ExecutionAttemptCreate(
                task_id=task.id,
                worker_id="worker-callback-race",
                retry_index=0,
                lease_token="lease-callback-race",
            ),
        )
        await ExecutionAttemptRepository.update(
            session,
            attempt.id,
            status=ExecutionAttemptStatus.RUNNING,
            started_at=datetime.utcnow() - timedelta(seconds=5),
            last_heartbeat_at=datetime.utcnow() - timedelta(seconds=5),
        )
        stored = await TaskRepository.get(session, task.id)
        assert stored is not None
        runtime_task = Task.model_validate(stored.model_dump())

    await worker_registry.register(WorkerInfo(worker_id="worker-callback-race", name="callback-race"))
    lease = await lock_backend.acquire(f"task:{task.id}", ttl=0.05)
    assert lease is not None

    finalize_task = asyncio.create_task(
        completion.finalize(
            runtime_task,
            TaskStatus.SUCCESS,
            result={"ok": True},
            finalize_latest_attempt=True,
        )
    )

    await dispatcher.started.wait()
    await asyncio.sleep(0.08)

    repaired = await reconciler.reconcile()
    dispatcher.release.set()
    updated = await finalize_task

    async with await get_session_no_context() as session:
        final_task = await TaskRepository.get(session, task.id)
        final_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

    assert repaired == 0
    assert updated is not None
    assert final_task is not None
    assert final_task.status == TaskStatus.SUCCESS
    assert final_task.result == {"ok": True}
    assert final_attempt is not None
    assert final_attempt.status == ExecutionAttemptStatus.SUCCEEDED
    assert len(dispatcher.payloads) == 1
    assert dispatcher.payloads[0]["status"] == TaskStatus.SUCCESS.value
    assert await queue_manager.dequeue(timeout=0.01) is None


@pytest.mark.asyncio
async def test_duplicate_finalize_overlap_with_callback_failure_dispatches_once_and_preserves_terminal_state() -> None:
    await drop_db()
    await init_db()

    dispatcher = FailingBlockingCallbackDispatcher()
    completion = TaskCompletionNode(callback_dispatcher=dispatcher)

    async with get_session() as session:
        task = await TaskRepository.create(
            session,
            TaskCreate(name="duplicate-finalize-overlap", payload={}, callback_url="https://callback/test"),
        )
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
                worker_id="worker-duplicate-finalize",
                retry_index=0,
                lease_token="lease-duplicate-finalize",
            ),
        )
        await ExecutionAttemptRepository.update(
            session,
            attempt.id,
            status=ExecutionAttemptStatus.RUNNING,
            started_at=datetime.utcnow() - timedelta(seconds=1),
            last_heartbeat_at=datetime.utcnow() - timedelta(seconds=1),
        )
        stored = await TaskRepository.get(session, task.id)
        assert stored is not None
        runtime_task = Task.model_validate(stored.model_dump())

    first_finalize = asyncio.create_task(
        completion.finalize(
            runtime_task,
            TaskStatus.SUCCESS,
            result={"ok": True},
            finalize_latest_attempt=True,
        )
    )
    await dispatcher.started.wait()
    second_finalize = asyncio.create_task(
        completion.finalize(
            runtime_task,
            TaskStatus.SUCCESS,
            result={"ok": True},
            finalize_latest_attempt=True,
        )
    )

    dispatcher.release.set()
    first_result, second_result = await asyncio.gather(first_finalize, second_finalize)

    async with await get_session_no_context() as session:
        final_task = await TaskRepository.get(session, task.id)
        final_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

    metrics = completion.get_metrics()
    assert first_result is not None
    assert second_result is not None
    assert final_task is not None
    assert final_task.status == TaskStatus.SUCCESS
    assert final_task.result == {"ok": True}
    assert final_attempt is not None
    assert final_attempt.status == ExecutionAttemptStatus.SUCCEEDED
    assert dispatcher.calls == 1
    assert len(dispatcher.payloads) == 1
    assert dispatcher.payloads[0]["status"] == TaskStatus.SUCCESS.value
    assert metrics is not None
    assert metrics.total_completed == 1
    assert metrics.callback_dispatches == 1
    assert metrics.callback_failures == 1


@pytest.mark.asyncio
async def test_partial_work_then_recovery_then_retry_budget_exhaustion_converges_to_failed() -> None:
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
            heartbeat_interval_seconds=0.2,
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

    side_effects = ["attempt-1"]

    async with get_session() as session:
        task = await TaskRepository.create(
            session,
            TaskCreate(name="partial-then-exhausted", payload={"value": 1}, max_retries=0),
        )
        await TaskRepository.update(
            session,
            task.id,
            status=TaskStatus.RUNNING,
            started_at=datetime.utcnow() - timedelta(seconds=5),
            updated_at=datetime.utcnow() - timedelta(seconds=5),
        )
        attempt = await ExecutionAttemptRepository.create(
            session,
            ExecutionAttemptCreate(
                task_id=task.id,
                worker_id="worker-partial-exhausted",
                retry_index=0,
                lease_token="lease-partial-exhausted",
            ),
        )
        await ExecutionAttemptRepository.update(
            session,
            attempt.id,
            status=ExecutionAttemptStatus.RUNNING,
            started_at=datetime.utcnow() - timedelta(seconds=5),
            last_heartbeat_at=datetime.utcnow() - timedelta(seconds=5),
        )

    await worker_registry.register(WorkerInfo(worker_id="worker-partial-exhausted", name="partial-exhausted"))
    lease = await lock_backend.acquire(f"task:{task.id}", ttl=0.05)
    assert lease is not None
    await asyncio.sleep(0.12)
    await worker_registry.deregister("worker-partial-exhausted")

    repaired = await reconciler.reconcile()
    assert repaired == 1

    queued_again = await queue_manager.dequeue(timeout=0.01)
    assert queued_again is not None
    assert queued_again.id == task.id

    async def handler(payload):
        side_effects.append("attempt-2")
        raise RuntimeError("boom-after-recovery")

    await worker_registry.register(WorkerInfo(worker_id="worker-partial-exhausted-2", name="partial-exhausted-2"))
    second_consumer = TaskConsumer(
        queue_manager=queue_manager,
        executor=TaskExecutor(),
        handler=handler,
        lock_backend=lock_backend,
        worker_id="worker-partial-exhausted-2",
        lease_ttl_seconds=1.0,
        heartbeat_interval_seconds=0.05,
    )

    await queue_manager.enqueue(queued_again)
    processed_second = await second_consumer._process_next_once()
    assert processed_second is True

    async with await get_session_no_context() as session:
        final_task = await TaskRepository.get(session, task.id)
        final_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

    assert side_effects == ["attempt-1", "attempt-2"]
    assert final_task is not None
    assert final_task.status == TaskStatus.FAILED
    assert final_task.error_message == "boom-after-recovery"
    assert final_task.retry_count == 1
    assert final_attempt is not None
    assert final_attempt.status == ExecutionAttemptStatus.FAILED
    assert final_attempt.error_message == "boom-after-recovery"
    assert await queue_manager.dequeue(timeout=0.01) is None


@pytest.mark.asyncio
async def test_callback_failure_with_lease_loss_and_reconciler_overlap_does_not_requeue_terminal_task() -> None:
    await drop_db()
    await init_db()

    factory = BackendFactory(
        BackendConfig(
            queue_type="redis",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
            distributed_mode=True,
            lease_ttl_seconds=0.05,
            heartbeat_interval_seconds=0.05,
        )
    )
    queue_manager = QueueManager(factory.create_queue_backend())
    lock_backend = factory.create_lock_backend()
    worker_registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=1.0)
    dispatcher = FailingBlockingCallbackDispatcher()
    completion = TaskCompletionNode(callback_dispatcher=dispatcher)
    reconciler = TaskReconciler(
        config=ReconciliationConfig(stuck_after_seconds=0, repair_strategy=RepairStrategy.REQUEUE),
        queue_manager=queue_manager,
        lock_backend=lock_backend,
        worker_registry=worker_registry,
    )

    async with get_session() as session:
        task = await TaskRepository.create(
            session,
            TaskCreate(name="callback-failure-reconciler-overlap", payload={}, callback_url="https://callback/test"),
        )
        await TaskRepository.update(
            session,
            task.id,
            status=TaskStatus.RUNNING,
            started_at=datetime.utcnow() - timedelta(seconds=5),
            updated_at=datetime.utcnow() - timedelta(seconds=5),
        )
        attempt = await ExecutionAttemptRepository.create(
            session,
            ExecutionAttemptCreate(
                task_id=task.id,
                worker_id="worker-callback-failure-overlap",
                retry_index=0,
                lease_token="lease-callback-failure-overlap",
            ),
        )
        await ExecutionAttemptRepository.update(
            session,
            attempt.id,
            status=ExecutionAttemptStatus.RUNNING,
            started_at=datetime.utcnow() - timedelta(seconds=5),
            last_heartbeat_at=datetime.utcnow() - timedelta(seconds=5),
        )
        stored = await TaskRepository.get(session, task.id)
        assert stored is not None
        runtime_task = Task.model_validate(stored.model_dump())

    await worker_registry.register(
        WorkerInfo(worker_id="worker-callback-failure-overlap", name="callback-failure-overlap")
    )
    lease = await lock_backend.acquire(f"task:{task.id}", ttl=0.05)
    assert lease is not None

    finalize_task = asyncio.create_task(
        completion.finalize(
            runtime_task,
            TaskStatus.SUCCESS,
            result={"ok": True},
            finalize_latest_attempt=True,
        )
    )

    await dispatcher.started.wait()
    await asyncio.sleep(0.08)

    repaired = await reconciler.reconcile()
    dispatcher.release.set()
    updated = await finalize_task

    async with await get_session_no_context() as session:
        final_task = await TaskRepository.get(session, task.id)
        final_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

    metrics = completion.get_metrics()
    assert repaired == 0
    assert updated is not None
    assert final_task is not None
    assert final_task.status == TaskStatus.SUCCESS
    assert final_task.result == {"ok": True}
    assert final_attempt is not None
    assert final_attempt.status == ExecutionAttemptStatus.SUCCEEDED
    assert await queue_manager.dequeue(timeout=0.01) is None
    assert dispatcher.calls == 1
    assert len(dispatcher.payloads) == 1
    assert metrics is not None
    assert metrics.callback_dispatches == 1
    assert metrics.callback_failures == 1


@pytest.mark.asyncio
async def test_concurrent_reconcile_does_not_double_requeue_same_orphan_task() -> None:
    await drop_db()
    await init_db()

    factory = BackendFactory(
        BackendConfig(
            queue_type="redis",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
            distributed_mode=True,
            lease_ttl_seconds=0.05,
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
        task = await TaskRepository.create(session, TaskCreate(name="orphan-once-only", payload={}))
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
                worker_id="worker-orphan-once-only",
                retry_index=0,
                lease_token="lease-orphan-once-only",
            ),
        )
        await ExecutionAttemptRepository.update(
            session,
            attempt.id,
            status=ExecutionAttemptStatus.RUNNING,
            started_at=datetime.utcnow() - timedelta(seconds=10),
            last_heartbeat_at=datetime.utcnow() - timedelta(seconds=10),
        )

    await worker_registry.register(WorkerInfo(worker_id="worker-orphan-once-only", name="orphan-once-only"))
    lease = await lock_backend.acquire(f"task:{task.id}", ttl=0.05)
    assert lease is not None
    await asyncio.sleep(0.12)
    await worker_registry.deregister("worker-orphan-once-only")

    repaired_first, repaired_second = await asyncio.gather(reconciler.reconcile(), reconciler.reconcile())

    async with await get_session_no_context() as session:
        final_task = await TaskRepository.get(session, task.id)
        final_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

    first_queued = await queue_manager.dequeue(timeout=0.01)
    second_queued = await queue_manager.dequeue(timeout=0.01)
    repair_history = reconciler.list_repair_history(limit=10, offset=0)

    assert sorted([repaired_first, repaired_second]) == [0, 1]
    assert final_task is not None
    assert final_task.status == TaskStatus.QUEUED
    assert final_task.retry_count == 1
    assert final_attempt is not None
    assert final_attempt.status == ExecutionAttemptStatus.ABANDONED
    assert first_queued is not None
    assert first_queued.id == task.id
    assert second_queued is None
    assert len(repair_history) == 1
    assert repair_history[0]["task_id"] == task.id
    assert repair_history[0]["action"] == "requeue"


@pytest.mark.asyncio
async def test_transient_heartbeat_extend_failure_converges_to_failed_abandoned() -> None:
    await drop_db()
    await init_db()

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
    raw_lock_backend = factory.create_lock_backend()
    lock_backend = ExtendFailOnceLockBackend(raw_lock_backend)

    async with get_session() as session:
        task = await TaskRepository.create(session, TaskCreate(name="transient-extend-failure", payload={}))
    await queue_manager.enqueue(task)

    async def slow_handler(payload):
        await asyncio.sleep(0.12)
        return {"ok": True}

    consumer = TaskConsumer(
        queue_manager=queue_manager,
        executor=TaskExecutor(),
        handler=slow_handler,
        lock_backend=lock_backend,
        worker_id="worker-transient-extend-failure",
        lease_ttl_seconds=1.0,
        heartbeat_interval_seconds=0.05,
    )

    processed = await consumer._process_next_once()
    assert processed is True

    async with await get_session_no_context() as session:
        final_task = await TaskRepository.get(session, task.id)
        final_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

    assert lock_backend.extend_calls >= 1
    assert final_task is not None
    assert final_task.status == TaskStatus.FAILED
    assert final_task.error_message == "lease lost during execution"
    assert final_attempt is not None
    assert final_attempt.status == ExecutionAttemptStatus.ABANDONED
    assert final_attempt.error_message == "lease lost during execution"


@pytest.mark.asyncio
async def test_worker_registry_loss_during_long_running_execution_does_not_trigger_repair_while_lease_is_live() -> None:
    await drop_db()
    await init_db()

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
    worker_registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=0.1)
    reconciler = TaskReconciler(
        config=ReconciliationConfig(stuck_after_seconds=0, repair_strategy=RepairStrategy.REQUEUE),
        queue_manager=queue_manager,
        lock_backend=lock_backend,
        worker_registry=worker_registry,
    )

    gate = asyncio.Event()

    async with get_session() as session:
        task = await TaskRepository.create(session, TaskCreate(name="live-lease-beats-dead-worker", payload={}))
    await queue_manager.enqueue(task)

    async def blocked_handler(payload):
        await gate.wait()
        return {"ok": True}

    worker_id = "worker-live-lease-beats-dead-worker"
    await worker_registry.register(WorkerInfo(worker_id=worker_id, name="live-lease-beats-dead-worker"))
    consumer = TaskConsumer(
        queue_manager=queue_manager,
        executor=TaskExecutor(),
        handler=blocked_handler,
        lock_backend=lock_backend,
        worker_id=worker_id,
        lease_ttl_seconds=1.0,
        heartbeat_interval_seconds=0.05,
    )

    process_task = asyncio.create_task(consumer._process_next_once())
    await asyncio.sleep(0.12)
    await worker_registry.deregister(worker_id)

    repaired = await reconciler.reconcile()

    async with await get_session_no_context() as session:
        running_task = await TaskRepository.get(session, task.id)
        running_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

    assert repaired == 0
    assert running_task is not None
    assert running_task.status == TaskStatus.RUNNING
    assert running_attempt is not None
    assert running_attempt.status in {
        ExecutionAttemptStatus.CLAIMED,
        ExecutionAttemptStatus.RUNNING,
    }

    gate.set()
    processed = await process_task
    assert processed is True

    async with await get_session_no_context() as session:
        final_task = await TaskRepository.get(session, task.id)
        final_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

    assert final_task is not None
    assert final_task.status == TaskStatus.SUCCESS
    assert final_attempt is not None
    assert final_attempt.status == ExecutionAttemptStatus.SUCCEEDED


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
