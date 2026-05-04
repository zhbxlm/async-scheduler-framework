"""Tests for task lifecycle."""

import asyncio
from datetime import datetime

import pytest

pytestmark = pytest.mark.mysql_required

import pytest_asyncio

from async_scheduler.core.models import (
    Task,
    TaskCreate,
    TaskStatus,
)
from async_scheduler.executor import TaskExecutor, default_task_handler
from async_scheduler.persistence import TaskRepository
from async_scheduler.queue import QueueManager


@pytest.mark.asyncio
class TestTaskLifecycle:
    """Tests for complete task lifecycle."""

    @pytest_asyncio.fixture
    async def db_session(self):
        """Create a database session for tests."""
        from async_scheduler.persistence import drop_db, get_session_no_context, init_db

        await drop_db()
        await init_db()

        async with await get_session_no_context() as session:
            yield session

    @pytest.fixture
    def task_create(self):
        """Create a task for testing."""
        return TaskCreate(
            name="test_task",
            payload={"test": "data"},
            priority=0,
            max_retries=2,
            timeout_seconds=30,
        )

    @pytest.fixture
    def queue_manager(self):
        """Create a queue manager."""
        return QueueManager()

    @pytest.fixture
    def executor(self):
        """Create a task executor."""
        return TaskExecutor()

    async def test_task_creation(self, db_session, task_create):
        """Test creating a new task."""
        task = await TaskRepository.create(db_session, task_create)

        assert task.id is not None
        assert task.name == "test_task"
        assert task.status == TaskStatus.PENDING
        assert task.priority == 0
        assert task.max_retries == 2
        assert task.timeout_seconds == 30
        assert task.retry_count == 0

    async def test_task_retrieval(self, db_session, task_create):
        """Test retrieving a task by ID."""
        created = await TaskRepository.create(db_session, task_create)
        retrieved = await TaskRepository.get(db_session, created.id)

        assert retrieved is not None
        assert retrieved.id == created.id
        assert retrieved.name == created.name

    async def test_task_list(self, db_session):
        """Test listing tasks."""
        # Create multiple tasks
        for i in range(5):
            task = TaskCreate(
                name=f"task_{i}",
                payload={"index": i},
            )
            await TaskRepository.create(db_session, task)

        # List all tasks
        all_tasks = await TaskRepository.list_all(db_session)
        assert len(all_tasks) == 5

        # List by status
        pending_tasks = await TaskRepository.list_all(db_session, status=TaskStatus.PENDING)
        assert len(pending_tasks) == 5

    async def test_task_update(self, db_session, task_create):
        """Test updating a task."""
        created = await TaskRepository.create(db_session, task_create)

        updated = await TaskRepository.update(
            db_session,
            created.id,
            status=TaskStatus.RUNNING,
        )

        assert updated is not None
        assert updated.status == TaskStatus.RUNNING

    async def test_task_deletion(self, db_session, task_create):
        """Test deleting a task."""
        created = await TaskRepository.create(db_session, task_create)

        deleted = await TaskRepository.delete(db_session, created.id)
        assert deleted is True

        retrieved = await TaskRepository.get(db_session, created.id)
        assert retrieved is None

    async def test_queue_enqueue_dequeue(self, queue_manager, task_create):
        """Test enqueuing and dequeuing tasks."""
        task = Task(**task_create.model_dump())

        await queue_manager.enqueue(task)

        # Should be able to dequeue
        dequeued = await queue_manager.dequeue(timeout=1.0)

        assert dequeued is not None
        assert dequeued.id == task.id
        assert dequeued.name == task.name

    async def test_queue_priority(self, queue_manager):
        """Test that higher priority tasks are dequeued first."""
        low_task = Task(
            name="low",
            priority=4,
            created_at=datetime.utcnow(),
        )
        high_task = Task(
            name="high",
            priority=-1,
            created_at=datetime.utcnow(),
        )

        await queue_manager.enqueue(low_task)
        await queue_manager.enqueue(high_task)

        # High priority should come first
        first = await queue_manager.dequeue(timeout=1.0)
        second = await queue_manager.dequeue(timeout=1.0)

        assert first.name == "high"
        assert second.name == "low"

    async def test_queue_cancellation(self, queue_manager, task_create):
        """Test cancelling a queued task."""
        task = Task(**task_create.model_dump())

        await queue_manager.enqueue(task)
        cancelled = await queue_manager.cancel(task.id)

        assert cancelled is True

    async def test_executor_success(self, executor):
        """Test successful task execution."""
        task = Task(
            name="test",
            payload={"sleep_seconds": 0.1},
            max_retries=2,
            timeout_seconds=10,
        )

        result = await executor.execute(task, default_task_handler)

        assert result.success is True
        assert result.error is None
        assert result.result is not None
        assert result.should_retry is False

    async def test_executor_retry(self, executor):
        """Test executor reports no further retry after retries are exhausted."""

        async def failing_handler(payload):
            raise Exception("Task failed")

        task = Task(
            name="failing",
            payload={},
            max_retries=2,
            timeout_seconds=10,
        )

        result = await executor.execute(task, failing_handler)

        assert result.success is False
        assert result.error is not None
        assert result.should_retry is False

    async def test_executor_timeout(self, executor):
        """Test executor timeout."""

        async def slow_handler(payload):
            await asyncio.sleep(100)  # Sleep longer than timeout

        task = Task(
            name="slow",
            payload={},
            max_retries=0,
            timeout_seconds=1,
        )

        result = await executor.execute(task, slow_handler)

        assert result.success is False
        assert isinstance(result.error, TimeoutError)

    async def test_executor_cancellation(self, executor):
        """Test executor cancellation."""

        async def cancellable_handler(payload):
            await asyncio.sleep(100)

        task = Task(
            name="cancellable",
            payload={},
            max_retries=0,
            timeout_seconds=100,
        )

        # Start execution
        execution = asyncio.create_task(executor.execute(task, cancellable_handler))

        # Cancel it
        await asyncio.sleep(0.1)
        cancelled = await executor.cancel(task.id)

        assert cancelled is True

        # Get result
        result = await execution
        assert result.success is False
        assert "cancelled" in str(result.error).lower()

    async def test_task_idempotency(self, db_session):
        """Test idempotent task creation."""
        task_create = TaskCreate(
            name="idem_task",
            payload={"value": 1},
            tenant_id="tenant-a",
            idempotency_key="order-123",
        )
        first = await TaskRepository.create(db_session, task_create)
        second = await TaskRepository.create(db_session, task_create)

        assert first.id == second.id

    async def test_idempotency_is_tenant_scoped(self, db_session):
        first = await TaskRepository.create(
            db_session,
            TaskCreate(
                name="idem_task",
                payload={"value": 1},
                tenant_id="tenant-a",
                idempotency_key="same-key",
            ),
        )
        second = await TaskRepository.create(
            db_session,
            TaskCreate(
                name="idem_task",
                payload={"value": 1},
                tenant_id="tenant-b",
                idempotency_key="same-key",
            ),
        )

        assert first.id != second.id

    async def test_full_lifecycle(self, db_session, queue_manager, executor):
        """Test complete task lifecycle from creation to completion."""
        # Create task
        task_create = TaskCreate(
            name="lifecycle_task",
            payload={"value": 42},
            priority=-1,
        )
        task = await TaskRepository.create(db_session, task_create)

        # Update to queued
        task.status = TaskStatus.QUEUED
        await TaskRepository.update(db_session, task.id, status=TaskStatus.QUEUED)

        # Enqueue
        await queue_manager.enqueue(task)

        # Dequeue
        task = await queue_manager.dequeue(timeout=1.0)
        assert task is not None

        # Update to running
        task.status = TaskStatus.RUNNING
        await TaskRepository.update(db_session, task.id, status=TaskStatus.RUNNING)

        # Execute
        result = await executor.execute(task, default_task_handler)

        assert result.success is True

        # Update to success
        task.status = TaskStatus.SUCCESS
        await TaskRepository.update(
            db_session,
            task.id,
            status=TaskStatus.SUCCESS,
            result=result.result,
        )

        # Verify final state
        final_task = await TaskRepository.get(db_session, task.id)
        assert final_task.status == TaskStatus.SUCCESS
        assert final_task.result is not None


if __name__ == "__main__":
    pass

