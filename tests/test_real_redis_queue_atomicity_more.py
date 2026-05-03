from __future__ import annotations

from datetime import datetime

import pytest

from async_scheduler.backends.redis import RedisQueueBackend
from async_scheduler.core.models import Task, TaskPriority, TaskStatus
from tests.fake_redis import FullFakeAsyncRedis


class RacingFakeAsyncRedis(FullFakeAsyncRedis):
    def __init__(self) -> None:
        super().__init__()
        self.on_lrem = None
        self.on_lpop = None
        self.on_zrem = None

    async def lrem(self, key: str, count: int, value: str) -> int:
        if self.on_lrem is not None:
            await self.on_lrem(key, value)
            self.on_lrem = None
        return await super().lrem(key, count, value)

    async def lpop(self, key: str):
        if self.on_lpop is not None:
            await self.on_lpop(key)
            self.on_lpop = None
        return await super().lpop(key)

    async def zrem(self, key: str, member: str) -> int:
        if self.on_zrem is not None:
            await self.on_zrem(key, member)
            self.on_zrem = None
        return await super().zrem(key, member)


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
async def test_cancel_returns_false_if_ready_entry_disappears_before_remove() -> None:
    client = RacingFakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)
    task = make_task("cancel-race")
    await backend.enqueue(task)

    # Simulate concurrent dequeue removing the task from the pending ZSET before cancel's zrem
    async def steal_before_zrem(key: str, member: str) -> None:
        for k in list(client.zsets.keys()):
            client.zsets[k].pop(member, None)

    client.on_zrem = steal_before_zrem
    cancelled = await backend.cancel(task.id)

    assert cancelled is False
    assert await backend.dequeue() is None


@pytest.mark.asyncio
async def test_dequeue_skips_cancelled_task_without_returning_duplicate() -> None:
    client = RacingFakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)
    task = make_task("dequeue-cancel-race", TaskPriority.HIGH)
    await backend.enqueue(task)

    # Cancel before dequeue runs
    await backend.cancel(task.id)
    popped = await backend.dequeue()

    assert popped is None
    assert await backend.dequeue() is None


@pytest.mark.asyncio
async def test_clear_removes_ready_entries_even_after_multiple_enqueues() -> None:
    client = RacingFakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)

    await backend.enqueue(make_task("c1", TaskPriority.HIGH))
    await backend.enqueue(make_task("c2", TaskPriority.HIGH))
    await backend.enqueue(make_task("c3", TaskPriority.NORMAL))
    await backend.clear()

    assert await backend.dequeue() is None
    sizes = await backend.size()
    assert all(count == 0 for count in sizes.values())
