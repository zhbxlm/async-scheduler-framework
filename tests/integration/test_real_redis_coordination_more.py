from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

from async_scheduler.backends.redis import RedisLockBackend, RedisQueueBackend
from async_scheduler.core.models import Task, TaskPriority, TaskStatus
from async_scheduler.distributed.worker_registry import WorkerInfo, WorkerRegistry



class SharedFakeAsyncRedis:
    def __init__(self) -> None:
        self.values: dict[str, tuple[str, int | None]] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.lists: dict[str, list[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False):
        if nx and key in self.values:
            return None
        self.values[key] = (value, ex)
        return True

    async def get(self, key: str):
        row = self.values.get(key)
        return None if row is None else row[0]

    async def delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self.values:
                del self.values[key]
                count += 1
            if key in self.hashes:
                del self.hashes[key]
                count += 1
            if key in self.lists:
                del self.lists[key]
                count += 1
            if key in self.zsets:
                del self.zsets[key]
                count += 1
        return count

    async def expire(self, key: str, ttl: int):
        if key in self.values:
            value, _ = self.values[key]
            self.values[key] = (value, ttl)
            return True
        if key in self.hashes:
            return True
        return False

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

    async def hset(self, key: str, mapping: dict[str, str]) -> int:
        self.hashes[key] = dict(mapping)
        return len(mapping)

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def exists(self, key: str) -> int:
        return 1 if key in self.hashes or key in self.values else 0

    async def keys(self, pattern: str) -> list[str]:
        prefix = pattern[:-1] if pattern.endswith("*") else pattern
        return [key for key in self.hashes if key.startswith(prefix)]


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
async def test_real_redis_coordination_promotes_delayed_task_on_shared_client() -> None:
    client = SharedFakeAsyncRedis()
    queue = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)

    await queue.enqueue(
        make_task("delayed-1", TaskPriority.HIGH),
        scheduled_at=datetime.utcnow() + timedelta(milliseconds=20),
    )

    assert await queue.dequeue() is None
    await asyncio.sleep(0.03)

    popped = await queue.dequeue()
    assert popped is not None
    assert popped.id == "delayed-1"


@pytest.mark.asyncio
async def test_real_redis_coordination_worker_heartbeat_and_lock_extend_can_progress_together() -> None:
    client = SharedFakeAsyncRedis()
    workers = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=30, client=client)
    lock = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)

    await workers.register(WorkerInfo(worker_id="worker-hb", name="heartbeat"))
    lease = await lock.acquire("task:hb-1", ttl=10)

    assert lease is not None
    assert await workers.heartbeat("worker-hb") is True
    assert await lock.extend(lease, ttl=20) is True
    assert await workers.is_live("worker-hb") is True
    assert await lock.is_locked("task:hb-1") is True


@pytest.mark.asyncio
async def test_real_redis_coordination_worker_disappears_after_deregister() -> None:
    client = SharedFakeAsyncRedis()
    workers = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=30, client=client)

    await workers.register(WorkerInfo(worker_id="worker-gone", name="gone"))
    assert await workers.is_live("worker-gone") is True

    assert await workers.deregister("worker-gone") is True
    assert await workers.is_live("worker-gone") is False
    assert await workers.list_workers() == []
