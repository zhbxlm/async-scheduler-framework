from __future__ import annotations

from datetime import datetime, timedelta

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
async def test_real_redis_queue_backend_enqueue_and_dequeue() -> None:
    client = FakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)

    task = make_task("task-1", TaskPriority.HIGH)
    await backend.enqueue(task)
    popped = await backend.dequeue()

    assert popped is not None
    assert popped.id == task.id


@pytest.mark.asyncio
async def test_real_redis_queue_backend_delayed_task_promotes_when_due() -> None:
    client = FakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)

    task = make_task("task-2")
    await backend.enqueue(task, scheduled_at=datetime.utcnow() + timedelta(milliseconds=20))

    before = await backend.dequeue()
    assert before is None

    await pytest.importorskip("asyncio").sleep(0.03)
    after = await backend.dequeue()
    assert after is not None
    assert after.id == task.id
