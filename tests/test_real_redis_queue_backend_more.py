from __future__ import annotations

import pytest

pytestmark = pytest.mark.redis_required


from async_scheduler.backends.redis import RedisQueueBackend
from async_scheduler.core.models import Task, TaskStatus
from tests.fake_redis import FullFakeAsyncRedis


class FakeAsyncRedis(FullFakeAsyncRedis):
    """Simple full-featured fake (no race injection)."""


def make_task(task_id: str, priority: int = 0) -> Task:
    return Task(
        id=task_id,
        name=f"task-{task_id}",
        payload={"task_id": task_id},
        priority=priority,
        status=TaskStatus.PENDING,
    )


@pytest.mark.asyncio
async def test_real_redis_queue_backend_cancel_removes_ready_task() -> None:
    client = FakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)
    task = make_task("cancel-ready")

    await backend.enqueue(task)
    cancelled = await backend.cancel(task.id)
    popped = await backend.dequeue()

    assert cancelled is True
    assert popped is None


@pytest.mark.asyncio
async def test_real_redis_queue_backend_update_priority_requeues_task() -> None:
    client = FakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)
    task = make_task("reprioritize", 4)

    await backend.enqueue(task)
    updated = await backend.update_priority(task.id, -1)
    popped = await backend.dequeue()

    assert updated is True
    assert popped is not None
    assert popped.id == task.id
    assert popped.priority == -1


@pytest.mark.asyncio
async def test_real_redis_queue_backend_size_reflects_ready_and_delayed() -> None:
    client = FakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)

    await backend.enqueue(make_task("high", -1))
    await backend.enqueue(make_task("normal", 0))

    sizes = await backend.size()

    assert sizes[-1] == 1
    assert sizes[0] == 1


@pytest.mark.asyncio
async def test_real_redis_queue_backend_clear_resets_all_keys() -> None:
    client = FakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)

    await backend.enqueue(make_task("t1", -1))
    await backend.enqueue(make_task("t2", 4))
    await backend.clear()

    assert await backend.dequeue() is None
    sizes = await backend.size()
    assert all(count == 0 for count in sizes.values())
