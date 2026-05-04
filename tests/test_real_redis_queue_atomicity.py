from __future__ import annotations

from datetime import datetime

import pytest

pytestmark = pytest.mark.redis_required


from async_scheduler.backends.redis import RedisQueueBackend
from async_scheduler.core.models import Task, TaskStatus
from tests.fake_redis import FullFakeAsyncRedis


class RacingFakeAsyncRedis(FullFakeAsyncRedis):
    def __init__(self) -> None:
        super().__init__()
        self.on_zrem = None
        self.on_lrem = None

    async def lrem(self, key: str, count: int, value: str) -> int:
        if self.on_lrem is not None:
            await self.on_lrem(key, value)
            self.on_lrem = None
        return await super().lrem(key, count, value)

    async def zrem(self, key: str, member: str) -> int:
        if self.on_zrem is not None:
            await self.on_zrem(key, member)
            self.on_zrem = None
        return await super().zrem(key, member)


def make_task(task_id: str, priority: int = 0) -> Task:
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

    task = make_task("race-reprio", 4)
    await backend.enqueue(task)

    # Simulate another worker dequeuing the task between hset and zrem
    async def steal_before_zrem(key: str, member: str) -> None:
        # Remove from all cap pending zsets (simulate concurrent dequeue)
        for k in list(client.zsets.keys()):
            client.zsets[k].pop(member, None)

    client.on_zrem = steal_before_zrem
    updated = await backend.update_priority(task.id, -1)

    assert updated is False
    assert await backend.dequeue() is None


@pytest.mark.asyncio
async def test_normal_reprioritize_still_moves_task_once() -> None:
    client = RacingFakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)

    task = make_task("normal-reprio", 4)
    await backend.enqueue(task)

    assert await backend.update_priority(task.id, -1) is True
    popped = await backend.dequeue()
    assert popped is not None
    assert popped.id == task.id
    assert await backend.dequeue() is None
