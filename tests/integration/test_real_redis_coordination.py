from __future__ import annotations

from datetime import datetime

import pytest

from async_scheduler.backends.redis import (
    RedisCompletionDedupBackend,
    RedisLockBackend,
    RedisQueueBackend,
)
from async_scheduler.core.models import Task, TaskStatus
from async_scheduler.distributed.worker_registry import WorkerInfo, WorkerRegistry


class SharedFakeAsyncRedis:
    def __init__(self) -> None:
        self.values: dict[str, tuple[str, int | None]] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.lists: dict[str, list[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    async def set(self, key: str, value: str, ex: int | None = None, px: int | None = None, nx: bool = False):
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


    async def pexpire(self, key: str, milliseconds: int) -> int:
        """Millisecond expire - store as seconds for simplicity."""
        return await self.expire(key, max(1, milliseconds // 1000))
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

    async def flushdb(self) -> None:
        self.values.clear()
        self.hashes.clear()
        self.lists.clear()
        self.zsets.clear()


def make_task(task_id: str) -> Task:
    return Task(
        id=task_id,
        name=f"task-{task_id}",
        payload={"task_id": task_id},
        priority=0,
        status=TaskStatus.PENDING,
        created_at=datetime.utcnow(),
    )


@pytest.mark.asyncio
@pytest.mark.redis_required
async def test_real_redis_coordination_chain_converges_on_shared_client() -> None:
    client = SharedFakeAsyncRedis()
    queue = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)
    lock = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)
    dedup = RedisCompletionDedupBackend(redis_url="redis://localhost:6379/0", client=client)
    workers = WorkerRegistry(redis_url="redis://localhost:6379/0", client=client)

    await workers.register(WorkerInfo(worker_id="worker-1", name="primary"))
    assert await workers.is_live("worker-1") is True

    task = make_task("coord-1")
    await queue.enqueue(task)
    candidate = await queue.dequeue()

    assert candidate is not None
    assert candidate.id == task.id

    lease = await lock.acquire(f"task:{task.id}", ttl=30)
    assert lease is not None

    first = await dedup.claim_once(f"{task.id}:success", ttl_seconds=60)
    second = await dedup.claim_once(f"{task.id}:success", ttl_seconds=60)

    assert first is True
    assert second is False
    assert await workers.is_live("worker-1") is True

    released = await lock.release(lease)
    assert released is True


@pytest.mark.asyncio
@pytest.mark.redis_required
async def test_real_redis_coordination_allows_reacquire_after_release() -> None:
    client = SharedFakeAsyncRedis()
    lock_a = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)
    lock_b = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)

    lease_a = await lock_a.acquire("task:reacquire", ttl=30)
    blocked = await lock_b.acquire("task:reacquire", ttl=30)
    assert lease_a is not None
    assert blocked is None

    assert await lock_a.release(lease_a) is True
    lease_b = await lock_b.acquire("task:reacquire", ttl=30)
    assert lease_b is not None
