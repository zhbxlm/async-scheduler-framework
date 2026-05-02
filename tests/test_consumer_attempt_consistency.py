from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from async_scheduler.backends.factory import BackendConfig, BackendFactory
from async_scheduler.core.consumer import TaskConsumer
from async_scheduler.core.models import ExecutionAttemptStatus, Task, TaskCreate, TaskStatus
from async_scheduler.executor import TaskExecutor
from async_scheduler.persistence import (
    ExecutionAttemptRepository,
    TaskRepository,
    drop_db,
    get_session,
    get_session_no_context,
    init_db,
)
from async_scheduler.queue import QueueManager


@pytest.mark.asyncio
class TestConsumerAttemptConsistency:
    @pytest_asyncio.fixture(autouse=True)
    async def init_database(self):
        await drop_db()
        await init_db()
        yield

    async def test_consumer_success_converges_task_and_attempt(self) -> None:
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

        async def handler(payload):
            return {"ok": payload["value"]}

        consumer = TaskConsumer(
            queue_manager=queue_manager,
            executor=TaskExecutor(),
            handler=handler,
            lock_backend=lock_backend,
            worker_id="worker-consistency",
            lease_ttl_seconds=1.0,
            heartbeat_interval_seconds=0.05,
        )

        async with get_session() as session:
            task = await TaskRepository.create(session, TaskCreate(name="consumer-consistency", payload={"value": 7}))
            queued = Task.model_validate(task.model_dump())
            queued.status = TaskStatus.QUEUED
            await TaskRepository.update(session, task.id, status=TaskStatus.QUEUED)
            await queue_manager.enqueue(queued)

        processed = await consumer._process_next_once()
        assert processed is True

        async with await get_session_no_context() as session:
            stored_task = await TaskRepository.get(session, task.id)
            latest_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

        assert stored_task is not None
        assert stored_task.status == TaskStatus.SUCCESS
        assert stored_task.result == {"ok": 7}
        assert latest_attempt is not None
        assert latest_attempt.status == ExecutionAttemptStatus.SUCCEEDED
        assert latest_attempt.result_payload == {"ok": 7}

    async def test_consumer_failure_converges_task_and_attempt(self) -> None:
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

        async def handler(payload):
            raise RuntimeError(f"boom-{payload['value']}")

        consumer = TaskConsumer(
            queue_manager=queue_manager,
            executor=TaskExecutor(),
            handler=handler,
            lock_backend=lock_backend,
            worker_id="worker-failure",
            lease_ttl_seconds=1.0,
            heartbeat_interval_seconds=0.05,
        )

        async with get_session() as session:
            task = await TaskRepository.create(session, TaskCreate(name="consumer-failure", payload={"value": 9}))
            queued = Task.model_validate(task.model_dump())
            queued.status = TaskStatus.QUEUED
            await TaskRepository.update(session, task.id, status=TaskStatus.QUEUED)
            await queue_manager.enqueue(queued)

        processed = await consumer._process_next_once()
        assert processed is True

        async with await get_session_no_context() as session:
            stored_task = await TaskRepository.get(session, task.id)
            latest_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

        assert stored_task is not None
        assert stored_task.status == TaskStatus.FAILED
        assert stored_task.error_message == "boom-9"
        assert latest_attempt is not None
        assert latest_attempt.status == ExecutionAttemptStatus.FAILED
        assert latest_attempt.error_message == "boom-9"

    async def test_consumer_exhausted_retries_converges_to_failed_without_requeue(self) -> None:
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

        async def handler(payload):
            raise RuntimeError(f"boom-{payload['value']}")

        consumer = TaskConsumer(
            queue_manager=queue_manager,
            executor=TaskExecutor(),
            handler=handler,
            lock_backend=lock_backend,
            worker_id="worker-exhausted-retries",
            lease_ttl_seconds=1.0,
            heartbeat_interval_seconds=0.05,
        )

        async with get_session() as session:
            task = await TaskRepository.create(
                session,
                TaskCreate(name="consumer-exhausted-retries", payload={"value": 11}, max_retries=0),
            )
            queued = Task.model_validate(task.model_dump())
            queued.status = TaskStatus.QUEUED
            await TaskRepository.update(session, task.id, status=TaskStatus.QUEUED)
            await queue_manager.enqueue(queued)

        processed = await consumer._process_next_once()
        assert processed is True

        async with await get_session_no_context() as session:
            stored_task = await TaskRepository.get(session, task.id)
            latest_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

        assert stored_task is not None
        assert stored_task.status == TaskStatus.FAILED
        assert stored_task.error_message == "boom-11"
        assert latest_attempt is not None
        assert latest_attempt.status == ExecutionAttemptStatus.FAILED
        assert latest_attempt.error_message == "boom-11"
        assert await queue_manager.dequeue(timeout=0.01) is None

    async def test_consumer_lease_loss_converges_to_failed_and_abandoned_attempt(self) -> None:
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

        async def handler(payload):
            await asyncio.sleep(0.2)
            return {"ok": payload["value"]}

        consumer = TaskConsumer(
            queue_manager=queue_manager,
            executor=TaskExecutor(),
            handler=handler,
            lock_backend=lock_backend,
            worker_id="worker-lease-loss",
            lease_ttl_seconds=0.08,
            heartbeat_interval_seconds=0.2,
        )

        async with get_session() as session:
            task = await TaskRepository.create(session, TaskCreate(name="consumer-lease-loss", payload={"value": 5}))
            queued = Task.model_validate(task.model_dump())
            queued.status = TaskStatus.QUEUED
            await TaskRepository.update(session, task.id, status=TaskStatus.QUEUED)
            await queue_manager.enqueue(queued)

        processed = await consumer._process_next_once()
        assert processed is True

        async with await get_session_no_context() as session:
            stored_task = await TaskRepository.get(session, task.id)
            latest_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

        assert stored_task is not None
        assert stored_task.status == TaskStatus.FAILED
        assert stored_task.error_message == "lease lost during execution"
        assert latest_attempt is not None
        assert latest_attempt.status == ExecutionAttemptStatus.ABANDONED
        assert latest_attempt.error_message == "lease lost during execution"
