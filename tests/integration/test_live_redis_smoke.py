from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta

import pytest

redis_asyncio = pytest.importorskip("redis.asyncio")

from async_scheduler.backends.redis import (  # noqa: E402
    RedisCompletionDedupBackend,
    RedisLockBackend,
    RedisQueueBackend,
)
from async_scheduler.core.models import Task, TaskPriority, TaskStatus  # noqa: E402
from async_scheduler.distributed.worker_registry import WorkerInfo, WorkerRegistry  # noqa: E402


LIVE_REDIS_URL = os.getenv("TEST_REDIS_URL")
pytestmark = pytest.mark.skipif(not LIVE_REDIS_URL, reason="TEST_REDIS_URL not set")


async def _build_live_client(url: str):
    client = redis_asyncio.Redis.from_url(url, decode_responses=True)
    try:
        await client.ping()
    except Exception:
        await client.aclose()
        raise
    return client


def make_task(task_id: str) -> Task:
    return Task(
        id=task_id,
        name=f"task-{task_id}",
        payload={"task_id": task_id},
        priority=TaskPriority.HIGH,
        status=TaskStatus.PENDING,
        created_at=datetime.utcnow(),
    )


@pytest.mark.asyncio
async def test_live_redis_coordination_smoke() -> None:
    client = await _build_live_client(LIVE_REDIS_URL)
    try:
        await client.flushdb()

        queue = RedisQueueBackend(redis_url=LIVE_REDIS_URL, client=client)
        lock = RedisLockBackend(redis_url=LIVE_REDIS_URL, client=client)
        dedup = RedisCompletionDedupBackend(redis_url=LIVE_REDIS_URL, client=client)
        workers = WorkerRegistry(redis_url=LIVE_REDIS_URL, client=client)

        await workers.register(WorkerInfo(worker_id="worker-live", name="live"))
        assert await workers.is_live("worker-live") is True

        task = make_task("live-smoke-1")
        await queue.enqueue(task)
        popped = await queue.dequeue()
        assert popped is not None
        assert popped.id == task.id

        lease = await lock.acquire(f"task:{task.id}", ttl=30)
        assert lease is not None
        assert await lock.extend(lease, ttl=45) is True

        first = await dedup.claim_once(f"{task.id}:success", ttl_seconds=60)
        second = await dedup.claim_once(f"{task.id}:success", ttl_seconds=60)
        assert first is True
        assert second is False

        assert await lock.release(lease) is True
    finally:
        await client.flushdb()
        await client.aclose()


@pytest.mark.asyncio
async def test_live_redis_queue_atomic_paths_smoke() -> None:
    client = await _build_live_client(LIVE_REDIS_URL)
    try:
        await client.flushdb()
        queue = RedisQueueBackend(redis_url=LIVE_REDIS_URL, client=client)

        delayed = make_task("live-delayed")
        await queue.enqueue(delayed, scheduled_at=datetime.utcnow() + timedelta(milliseconds=20))
        assert await queue.dequeue() is None
        await asyncio.sleep(0.03)
        promoted = await queue.dequeue()
        assert promoted is not None
        assert promoted.id == delayed.id

        reprio = make_task("live-reprio", TaskPriority.LOW)
        await queue.enqueue(reprio)
        assert await queue.update_priority(reprio.id, TaskPriority.HIGH) is True
        reprio_popped = await queue.dequeue()
        assert reprio_popped is not None
        assert reprio_popped.id == reprio.id
        assert reprio_popped.priority == TaskPriority.HIGH

        cancelled = make_task("live-cancel", TaskPriority.NORMAL)
        await queue.enqueue(cancelled)
        assert await queue.cancel(cancelled.id) is True
        assert await queue.dequeue() is None
    finally:
        await client.flushdb()
        await client.aclose()


@pytest.mark.asyncio
async def test_live_redis_two_workers_compete_for_single_lock() -> None:
    client = await _build_live_client(LIVE_REDIS_URL)
    try:
        await client.flushdb()
        lock_a = RedisLockBackend(redis_url=LIVE_REDIS_URL, client=client)
        lock_b = RedisLockBackend(redis_url=LIVE_REDIS_URL, client=client)

        lease_a = await lock_a.acquire("task:live-race", ttl=30)
        lease_b = await lock_b.acquire("task:live-race", ttl=30)

        assert lease_a is not None
        assert lease_b is None
        assert await lock_a.release(lease_a) is True
        assert await lock_b.acquire("task:live-race", ttl=30) is not None
    finally:
        await client.flushdb()
        await client.aclose()
