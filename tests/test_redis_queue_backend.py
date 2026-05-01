from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from async_scheduler.backends.factory import BackendConfig, BackendFactory
from async_scheduler.core.models import Task, TaskPriority


@pytest.mark.asyncio
class TestRedisQueueBackend:
    async def test_factory_can_create_redis_queue_backend(self) -> None:
        config = BackendConfig(
            queue_type="redis",
            lock_type="memory",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )

        backend = BackendFactory(config).create_queue_backend()

        assert backend is not None
        assert backend.__class__.__name__ == "RedisQueueBackend"

    async def test_enqueue_and_dequeue_immediate_task(self) -> None:
        config = BackendConfig(
            queue_type="redis",
            lock_type="memory",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )
        backend = BackendFactory(config).create_queue_backend()
        await backend.clear()

        task = Task(name="immediate", payload={"x": 1}, priority=TaskPriority.HIGH)
        await backend.enqueue(task)

        dequeued = await backend.dequeue(timeout=0.1)

        assert dequeued is not None
        assert dequeued.id == task.id
        assert backend.get_queue_count() == 0

    async def test_delayed_task_not_visible_before_ready(self) -> None:
        config = BackendConfig(
            queue_type="redis",
            lock_type="memory",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )
        backend = BackendFactory(config).create_queue_backend()
        await backend.clear()

        task = Task(name="delayed", payload={})
        scheduled_at = datetime.utcnow() + timedelta(seconds=30)
        await backend.enqueue(task, scheduled_at=scheduled_at)

        dequeued = await backend.dequeue(timeout=0.01)

        assert dequeued is None
        assert backend.is_scheduled(task.id) is True
        assert backend.get_scheduled_count() == 1

    async def test_due_delayed_task_is_promoted_on_dequeue(self) -> None:
        config = BackendConfig(
            queue_type="redis",
            lock_type="memory",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )
        backend = BackendFactory(config).create_queue_backend()
        await backend.clear()

        task = Task(name="due-delayed", payload={}, priority=TaskPriority.CRITICAL)
        scheduled_at = datetime.utcnow() - timedelta(seconds=1)
        await backend.enqueue(task, scheduled_at=scheduled_at)

        dequeued = await backend.dequeue(timeout=0.1)

        assert dequeued is not None
        assert dequeued.id == task.id
        assert backend.get_scheduled_count() == 0

    async def test_requeue_via_priority_update_keeps_task_available(self) -> None:
        config = BackendConfig(
            queue_type="redis",
            lock_type="memory",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )
        backend = BackendFactory(config).create_queue_backend()
        await backend.clear()

        task = Task(name="reprioritize", payload={}, priority=TaskPriority.LOW)
        await backend.enqueue(task)

        updated = await backend.update_priority(task.id, TaskPriority.CRITICAL)
        peeked = await backend.peek(limit=1)

        assert updated is True
        assert len(peeked) == 1
        assert peeked[0].id == task.id
        assert peeked[0].priority == TaskPriority.CRITICAL

    async def test_cancel_removes_queued_or_delayed_task(self) -> None:
        config = BackendConfig(
            queue_type="redis",
            lock_type="memory",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )
        backend = BackendFactory(config).create_queue_backend()
        await backend.clear()

        queued_task = Task(name="queued", payload={})
        delayed_task = Task(name="delayed", payload={})
        await backend.enqueue(queued_task)
        await backend.enqueue(delayed_task, scheduled_at=datetime.utcnow() + timedelta(seconds=60))

        cancelled_queued = await backend.cancel(queued_task.id)
        cancelled_delayed = await backend.cancel(delayed_task.id)
        dequeued = await backend.dequeue(timeout=0.01)

        assert cancelled_queued is True
        assert cancelled_delayed is True
        assert dequeued is None
        assert backend.get_queue_count() == 0
        assert backend.get_scheduled_count() == 0

    async def test_queue_stats_reflect_state(self) -> None:
        config = BackendConfig(
            queue_type="redis",
            lock_type="memory",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )
        backend = BackendFactory(config).create_queue_backend()
        await backend.clear()

        high = Task(name="high", payload={}, priority=TaskPriority.HIGH)
        normal = Task(name="normal", payload={}, priority=TaskPriority.NORMAL)
        delayed = Task(name="delayed", payload={}, priority=TaskPriority.LOW)
        await backend.enqueue(high)
        await backend.enqueue(normal)
        await backend.enqueue(delayed, scheduled_at=datetime.utcnow() + timedelta(seconds=60))

        sizes = await backend.size()

        assert sizes[TaskPriority.HIGH.value] == 1
        assert sizes[TaskPriority.NORMAL.value] == 1
        assert backend.get_queue_count() == 2
        assert backend.get_scheduled_count() == 1
