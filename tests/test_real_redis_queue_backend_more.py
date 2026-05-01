from __future__ import annotations

import pytest

from async_scheduler.backends.redis import RedisQueueBackend
from async_scheduler.core.models import Task, TaskPriority, TaskStatus


class FakeAsyncRedis:
    def __init__(self) -> None:
        self.lists: dict[str, list[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    async def rpush(self, key: str, value: str) -> int:
        self.lists.setdefault(key, []).append(value)
        return len(self.lists[key])

    async def lpop(self, key: str):
        values = self.lists.get(key, [])
        if not values:
            return None
        return values.pop(0)

    async def llen(self, key: str) -> int:
        return len(self.lists.get(key, []))

    async def lrange(self, key: str, start: int, stop: int):
        values = self.lists.get(key, [])
        if stop == -1:
            return values[start:]
        return values[start : stop + 1]

    async def lrem(self, key: str, count: int, value: str) -> int:
        values = self.lists.get(key, [])
        removed = 0
        kept: list[str] = []
        for item in values:
            if item == value and (count == 0 or removed < count):
                removed += 1
                continue
            kept.append(item)
        self.lists[key] = kept
        return removed

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[member] = score
        return len(mapping)

    async def zrangebyscore(self, key: str, min_score: float, max_score: float):
        bucket = self.zsets.get(key, {})
        return [member for member, score in bucket.items() if min_score <= score <= max_score]

    async def zrem(self, key: str, member: str) -> int:
        bucket = self.zsets.get(key, {})
        existed = member in bucket
        bucket.pop(member, None)
        return 1 if existed else 0

    async def zcard(self, key: str) -> int:
        return len(self.zsets.get(key, {}))

    async def delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self.lists:
                del self.lists[key]
                count += 1
            if key in self.zsets:
                del self.zsets[key]
                count += 1
        return count


def make_task(task_id: str, priority: TaskPriority = TaskPriority.NORMAL) -> Task:
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
    task = make_task("reprioritize", TaskPriority.LOW)

    await backend.enqueue(task)
    updated = await backend.update_priority(task.id, TaskPriority.HIGH)
    popped = await backend.dequeue()

    assert updated is True
    assert popped is not None
    assert popped.id == task.id
    assert popped.priority == TaskPriority.HIGH


@pytest.mark.asyncio
async def test_real_redis_queue_backend_size_reflects_ready_and_delayed() -> None:
    client = FakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)

    await backend.enqueue(make_task("high", TaskPriority.HIGH))
    await backend.enqueue(make_task("normal", TaskPriority.NORMAL))

    sizes = await backend.size()

    assert sizes[TaskPriority.HIGH.value] == 1
    assert sizes[TaskPriority.NORMAL.value] == 1


@pytest.mark.asyncio
async def test_real_redis_queue_backend_clear_resets_all_keys() -> None:
    client = FakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)

    await backend.enqueue(make_task("t1", TaskPriority.HIGH))
    await backend.enqueue(make_task("t2", TaskPriority.LOW))
    await backend.clear()

    assert await backend.dequeue() is None
    sizes = await backend.size()
    assert all(count == 0 for count in sizes.values())
