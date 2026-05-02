from __future__ import annotations

import asyncio
import os
from datetime import datetime

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
async def test_live_redis_multi_worker_claim_and_duplicate_completion_converges() -> None:
    client = await _build_live_client(LIVE_REDIS_URL)
    try:
        await client.flushdb()

        queue = RedisQueueBackend(redis_url=LIVE_REDIS_URL, client=client)
        queue_manager = QueueManager(queue)
        lock_a = RedisLockBackend(redis_url=LIVE_REDIS_URL, client=client)
        lock_b = RedisLockBackend(redis_url=LIVE_REDIS_URL, client=client)
        dedup = RedisCompletionDedupBackend(redis_url=LIVE_REDIS_URL, client=client)
        node_a = TaskCompletionNode(dedup_backend=dedup)
        node_b = TaskCompletionNode(dedup_backend=dedup)
        workers = WorkerRegistry(redis_url=LIVE_REDIS_URL, client=client)

        await workers.register(WorkerInfo(worker_id="worker-a", name="a"))
        await workers.register(WorkerInfo(worker_id="worker-b", name="b"))

        task = make_task("live-multi-overlap", TaskPriority.HIGH)
        await queue_manager.enqueue(task)

        candidate_a, candidate_b = await asyncio.gather(
            queue_manager.dequeue(),
            queue_manager.dequeue(),
        )

        candidates = [candidate for candidate in (candidate_a, candidate_b) if candidate is not None]
        assert len(candidates) == 1
        candidate = candidates[0]

        lease_a, lease_b = await asyncio.gather(
            lock_a.acquire(f"task:{task.id}", ttl=30),
            lock_b.acquire(f"task:{task.id}", ttl=30),
        )
        assert (lease_a is None) != (lease_b is None)

        winner = node_a if lease_a is not None else node_b
        loser = node_b if lease_a is not None else node_a

        result_a, result_b = await asyncio.gather(
            winner.finalize(candidate, TaskStatus.SUCCESS, result={"winner": True}),
            loser.finalize(candidate, TaskStatus.SUCCESS, result={"winner": False}),
        )

        stored_result = result_a or result_b
        assert stored_result is not None
        assert stored_result.status == TaskStatus.SUCCESS
        assert stored_result.result in ({"winner": True}, {"winner": False})
        assert await queue_manager.dequeue() is None
    finally:
        await client.flushdb()
        await client.aclose()
