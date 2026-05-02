from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta

import pytest

redis_asyncio = pytest.importorskip("redis.asyncio")

from async_scheduler.backends.redis import RedisCompletionDedupBackend, RedisLockBackend, RedisQueueBackend  # noqa: E402
from async_scheduler.core.models import Task, TaskPriority, TaskStatus  # noqa: E402
from async_scheduler.distributed.worker_registry import WorkerInfo, WorkerRegistry  # noqa: E402
from async_scheduler.platform.completion import TaskCompletionNode  # noqa: E402
from async_scheduler.queue import QueueManager  # noqa: E402


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
async def test_live_redis_multi_worker_dead_owner_recovery() -> None:
    client = await _build_live_client(LIVE_REDIS_URL)
    try:
        await client.flushdb()

        queue = RedisQueueBackend(redis_url=LIVE_REDIS_URL, client=client)
        queue_manager = QueueManager(queue)
        lock = RedisLockBackend(redis_url=LIVE_REDIS_URL, client=client)
        dedup = RedisCompletionDedupBackend(redis_url=LIVE_REDIS_URL, client=client)
        completion_node = TaskCompletionNode(dedup_backend=dedup)
        workers = WorkerRegistry(redis_url=LIVE_REDIS_URL, client=client)

        await workers.register(WorkerInfo(worker_id="worker-a", name="a"))
        await workers.register(WorkerInfo(worker_id="worker-b", name="b"))

        task = make_task("dead-owner-recovery", TaskPriority.HIGH)
        await queue_manager.enqueue(task)

        candidate = await queue_manager.dequeue()
        assert candidate is not None

        lease_a = await lock.acquire(f"task:{task.id}", ttl=1)
        assert lease_a is not None

        await asyncio.sleep(2)

        await workers.unregister("worker-a")
        await workers.register(WorkerInfo(worker_id="worker-c", name="c"))

        candidate = await queue_manager.dequeue()
        assert candidate is not None

        lease_c = await lock.acquire(f"task:{task.id}", ttl=30)
        assert lease_c is not None

        result = await completion_node.finalize(candidate, TaskStatus.SUCCESS, result={"recovered": True})
        assert result is not None
        assert result.status == TaskStatus.SUCCESS
        assert result.result == {"recovered": True}
        assert await queue_manager.dequeue() is None
    finally:
        await client.flushdb()
        await client.aclose()