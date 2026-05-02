from __future__ import annotations

import pytest
import pytest_asyncio

from async_scheduler.backends.factory import BackendConfig, BackendFactory
from async_scheduler.core.models import Task, TaskCreate, TaskStatus
from async_scheduler.executor import TaskExecutor
from async_scheduler.persistence import TaskRepository, drop_db, get_session, get_session_no_context, init_db
from async_scheduler.queue import QueueManager
from async_scheduler.worker.base import TaskWorker


class FailingTaskWorker(TaskWorker):
    async def process(self, payload: dict):
        raise RuntimeError(f"boom-{payload['value']}")


@pytest.mark.asyncio
class TestTaskWorkerRetrySemantics:
    @pytest_asyncio.fixture(autouse=True)
    async def init_database(self):
        await drop_db()
        await init_db()
        yield

    async def test_task_worker_failure_after_retry_budget_exhaustion_converges_to_failed(self) -> None:
        factory = BackendFactory(
            BackendConfig(
                queue_type="redis",
                lock_type="memory",
                registry_type="memory",
                redis_url="redis://localhost:6379/0",
            )
        )
        queue_manager = QueueManager(factory.create_queue_backend())
        worker = FailingTaskWorker(name="failing-worker", queue_manager=queue_manager, executor=TaskExecutor())

        async with get_session() as session:
            task = await TaskRepository.create(
                session,
                TaskCreate(name="task-worker-retry-exhaustion", payload={"value": 21}, max_retries=1),
            )
            queued = Task.model_validate(task.model_dump())
            queued.status = TaskStatus.QUEUED
            await TaskRepository.update(session, task.id, status=TaskStatus.QUEUED)
            await queue_manager.enqueue(queued)

        dequeued = await queue_manager.dequeue(timeout=0.01)
        assert dequeued is not None

        await worker._process_single_task(dequeued)

        async with await get_session_no_context() as session:
            stored_task = await TaskRepository.get(session, task.id)

        assert stored_task is not None
        assert stored_task.status == TaskStatus.FAILED
        assert stored_task.error_message == "boom-21"
        assert stored_task.retry_count == 1
        assert await queue_manager.dequeue(timeout=0.01) is None
