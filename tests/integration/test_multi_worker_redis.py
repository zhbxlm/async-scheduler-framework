from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from async_scheduler.backends.factory import BackendConfig, BackendFactory
from async_scheduler.core.consumer import TaskConsumer
from async_scheduler.core.models import Task, TaskCreate, TaskStatus
from async_scheduler.executor import TaskExecutor
from async_scheduler.persistence import drop_db, get_session, get_session_no_context, init_db, TaskRepository
from async_scheduler.queue import QueueManager


@pytest.mark.asyncio
async def test_multi_worker_claims_all_tasks_without_duplicate_execution() -> None:
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

    executed: list[int] = []

    async def handler(payload):
        await asyncio.sleep(0.05)
        executed.append(payload["index"])
        return {"index": payload["index"]}

    async with get_session() as session:
        for i in range(5):
            task = await TaskRepository.create(session, TaskCreate(name=f"task-{i}", payload={"index": i}))
            queued = Task.model_validate(task.model_dump())
            queued.status = TaskStatus.QUEUED
            await TaskRepository.update(session, task.id, status=TaskStatus.QUEUED)
            await queue_manager.enqueue(queued)

    workers = [
        TaskConsumer(
            queue_manager=queue_manager,
            executor=TaskExecutor(),
            handler=handler,
            lock_backend=lock_backend,
            worker_id=f"worker-{i}",
            lease_ttl_seconds=1.0,
            heartbeat_interval_seconds=0.05,
        )
        for i in range(3)
    ]

    await asyncio.gather(*(worker._process_next_once() for worker in workers for _ in range(2)))
    await asyncio.sleep(0.4)

    async with await get_session_no_context() as session:
        tasks = await TaskRepository.list_all(session, limit=50)

    created_tasks = [task for task in tasks if task.name.startswith("task-")]

    assert sorted(executed) == [0, 1, 2, 3, 4]
    assert len(executed) == 5
    assert len(created_tasks) == 5
    assert all(task.status in {TaskStatus.SUCCESS, TaskStatus.FAILED} for task in created_tasks)
