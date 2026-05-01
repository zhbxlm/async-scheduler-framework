from __future__ import annotations

from datetime import datetime

import pytest

from async_scheduler.backends.redis import RedisQueueBackend
from async_scheduler.core.models import Task, TaskPriority, TaskStatus


class RacingFakeAsyncRedis:
    def __init__(self) -> None:
        self.lists: dict[str, list[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.on_zrem = None
        self.on_lrem = None

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

    async def lrem(self, key: str, count: int, value: str) -> int:
        if self.on_lrem is not None:
            await self.on_lrem(key, value)
            self.on_lrem = None
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
        if self.on_zrem is not None:
            await self.on_zrem(key, member)
            self.on_zrem = None
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
        created_at=datetime.utcnow(),
    )


@pytest.mark.asyncio
async def test_delayed_promotion_does_not_duplicate_when_member_removed_by_race() -> None:
    client = RacingFakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)

    task = make_task("race-promo")
    await client.zadd("async-scheduler:queue:delayed", {backend._serialize_task(task): 0})

    async def steal_before_remove(key: str, member: str) -> None:
        client.zsets.get(key, {}).pop(member, None)

    client.on_zrem = steal_before_remove
    await backend._promote_due_tasks()

    assert await backend.dequeue() is None


@pytest.mark.asyncio
async def test_reprioritize_does_not_duplicate_when_old_entry_removed_by_race() -> None:
    client = RacingFakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)

    task = make_task("race-reprio", TaskPriority.LOW)
    await backend.enqueue(task)

    async def steal_before_remove(key: str, value: str) -> None:
        values = client.lists.get(key, [])
        client.lists[key] = [item for item in values if item != value]

    client.on_lrem = steal_before_remove
    updated = await backend.update_priority(task.id, TaskPriority.HIGH)

    assert updated is False
    assert await backend.dequeue() is None


@pytest.mark.asyncio
async def test_normal_reprioritize_still_moves_task_once() -> None:
    client = RacingFakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)

    task = make_task("normal-reprio", TaskPriority.LOW)
    await backend.enqueue(task)

    assert await backend.update_priority(task.id, TaskPriority.HIGH) is True
    popped = await backend.dequeue()
    assert popped is not None
    assert popped.id == task.id
    assert await backend.dequeue() is None
