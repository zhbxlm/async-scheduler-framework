from __future__ import annotations

from datetime import datetime

import pytest

fakeredis = pytest.importorskip("fakeredis.aioredis")

from async_scheduler.backends.redis import (
    RedisCompletionDedupBackend,
    RedisLockBackend,
    RedisQueueBackend,
)
from async_scheduler.core.models import Task, TaskStatus
from async_scheduler.distributed.worker_registry import WorkerInfo, WorkerRegistry


@pytest.mark.asyncio
async def test_fakeredis_coordination_chain() -> None:
    client = fakeredis.FakeRedis(decode_responses=True)
    queue = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)
    lock = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)
    dedup = RedisCompletionDedupBackend(redis_url="redis://localhost:6379/0", client=client)
    workers = WorkerRegistry(redis_url="redis://localhost:6379/0", client=client)

    task = Task(
        id="fakeredis-task-1",
        name="fakeredis-task-1",
        payload={"hello": "world"},
        priority=0,
        status=TaskStatus.PENDING,
        created_at=datetime.utcnow(),
    )

    await workers.register(WorkerInfo(worker_id="fakeredis-worker", name="fakeredis"))
    assert await workers.is_live("fakeredis-worker") is True

    await queue.enqueue(task)
    candidate = await queue.dequeue()
    assert candidate is not None
    assert candidate.id == task.id

    lease = await lock.acquire(f"task:{task.id}", ttl=30)
    assert lease is not None
    assert await lock.extend(lease, ttl=45) is True

    first = await dedup.claim_once(f"{task.id}:success", ttl_seconds=60)
    second = await dedup.claim_once(f"{task.id}:success", ttl_seconds=60)
    assert first is True
    assert second is False

    assert await lock.release(lease) is True
    assert await workers.deregister("fakeredis-worker") is True
