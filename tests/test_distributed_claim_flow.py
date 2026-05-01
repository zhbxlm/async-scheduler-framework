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
    get_session,
    get_session_no_context,
    init_db,
)
from async_scheduler.queue import QueueManager


@pytest.mark.asyncio
class TestDistributedClaimFlow:
    @pytest_asyncio.fixture(autouse=True)
    async def init_database(self):
        await init_db()
        yield

    async def test_two_consumers_race_and_only_one_executes(self) -> None:
        config = BackendConfig(
            queue_type="redis",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
            distributed_mode=True,
            lease_ttl_seconds=5.0,
            heartbeat_interval_seconds=0.05,
        )
        factory = BackendFactory(config)
        queue_manager = QueueManager(factory.create_queue_backend())
        lock_backend = factory.create_lock_backend()

        async with get_session() as session:
            task = await TaskRepository.create(session, TaskCreate(name="race-task", payload={"value": 1}))
            queued_task = Task.model_validate(task.model_dump())
            queued_task.status = TaskStatus.QUEUED
            await TaskRepository.update(session, task.id, status=TaskStatus.QUEUED)
        await queue_manager.enqueue(queued_task)

        executed_by: list[str] = []

        async def handler(payload):
            await asyncio.sleep(0.1)
            executed_by.append("handler")
            return {"ok": True}

        consumer_a = TaskConsumer(
            queue_manager=queue_manager,
            executor=TaskExecutor(),
            handler=handler,
            lock_backend=lock_backend,
            worker_id="worker-a",
            lease_ttl_seconds=5.0,
            heartbeat_interval_seconds=0.05,
        )
        consumer_b = TaskConsumer(
            queue_manager=queue_manager,
            executor=TaskExecutor(),
            handler=handler,
            lock_backend=lock_backend,
            worker_id="worker-b",
            lease_ttl_seconds=5.0,
            heartbeat_interval_seconds=0.05,
        )

        await asyncio.gather(
            consumer_a._process_next_once(),
            consumer_b._process_next_once(),
        )
        await asyncio.sleep(0.25)

        async with await get_session_no_context() as session:
            attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)
            final_task = await TaskRepository.get(session, task.id)

        assert len(executed_by) == 1
        assert attempt is not None
        assert attempt.worker_id in {"worker-a", "worker-b"}
        assert attempt.status == ExecutionAttemptStatus.SUCCEEDED
        assert final_task is not None
        assert final_task.status == TaskStatus.SUCCESS

    async def test_heartbeat_extends_active_lease_and_updates_attempt(self) -> None:
        config = BackendConfig(
            queue_type="redis",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
            distributed_mode=True,
            lease_ttl_seconds=0.15,
            heartbeat_interval_seconds=0.05,
        )
        factory = BackendFactory(config)
        queue_manager = QueueManager(factory.create_queue_backend())
        lock_backend = factory.create_lock_backend()

        async with get_session() as session:
            task = await TaskRepository.create(session, TaskCreate(name="heartbeat-task", payload={}))
            queued_task = Task.model_validate(task.model_dump())
            queued_task.status = TaskStatus.QUEUED
            await TaskRepository.update(session, task.id, status=TaskStatus.QUEUED)
        await queue_manager.enqueue(queued_task)

        release_gate = asyncio.Event()

        async def handler(payload):
            await asyncio.sleep(0.22)
            release_gate.set()
            return {"ok": True}

        consumer = TaskConsumer(
            queue_manager=queue_manager,
            executor=TaskExecutor(),
            handler=handler,
            lock_backend=lock_backend,
            worker_id="worker-heartbeat",
            lease_ttl_seconds=0.15,
            heartbeat_interval_seconds=0.05,
        )

        await consumer._process_next_once()
        await release_gate.wait()
        await asyncio.sleep(0.05)

        async with await get_session_no_context() as session:
            attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

        assert attempt is not None
        assert attempt.status in {ExecutionAttemptStatus.SUCCEEDED, ExecutionAttemptStatus.ABANDONED}
        assert attempt.last_heartbeat_at is not None
        assert attempt.started_at is not None
        assert attempt.completed_at is not None
