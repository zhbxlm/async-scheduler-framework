from __future__ import annotations

import asyncio
import os
from datetime import datetime
from typing import Any

import pytest

redis_asyncio = pytest.importorskip("redis.asyncio")

from async_scheduler.backends.redis import RedisCompletionDedupBackend, RedisLockBackend, RedisQueueBackend  # noqa: E402
from async_scheduler.core.models import Task, TaskPriority, TaskStatus  # noqa: E402
from async_scheduler.distributed.worker_registry import WorkerInfo, WorkerRegistry  # noqa: E402
from async_scheduler.platform.completion import TaskCompletionNode  # noqa: E402
from async_scheduler.platform.reconciler import ReconciliationConfig, RepairStrategy, TaskReconciler  # noqa: E402
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


def make_task(task_id: str, priority: TaskPriority = TaskPriority.NORMAL, payload: dict[str, Any] | None = None) -> Task:
    return Task(
        id=task_id,
        name=f"task-{task_id}",
        payload=payload or {"task_id": task_id},
        priority=priority,
        status=TaskStatus.PENDING,
        created_at=datetime.utcnow(),
    )


@pytest.mark.asyncio
async def test_live_redis_end_to_end_consumer_loop() -> None:
    client = await _build_live_client(LIVE_REDIS_URL)
    try:
        await client.flushdb()

        queue = RedisQueueBackend(redis_url=LIVE_REDIS_URL, client=client)
        queue_manager = QueueManager(queue)
        lock = RedisLockBackend(redis_url=LIVE_REDIS_URL, client=client)
        dedup = RedisCompletionDedupBackend(redis_url=LIVE_REDIS_URL, client=client)
        completion_node = TaskCompletionNode(dedup_backend=dedup)
        workers = WorkerRegistry(redis_url=LIVE_REDIS_URL, client=client)

        reconciler = TaskReconciler(
            config=ReconciliationConfig(stuck_after_seconds=0, repair_strategy=RepairStrategy.REQUEUE),
            queue_manager=queue_manager,
            lock_backend=lock,
            worker_registry=workers,
        )

        await workers.register(WorkerInfo(worker_id="worker-e2e", name="e2e"))

        task1 = make_task("e2e-task-1", TaskPriority.HIGH, {"value": 1})
        task2 = make_task("e2e-task-2", TaskPriority.NORMAL, {"value": 2})
        await queue_manager.enqueue(task1)
        await queue_manager.enqueue(task2)

        candidate1 = await queue_manager.dequeue()
        assert candidate1 is not None
        assert candidate1.id == "e2e-task-1"

        lease1 = await lock.acquire(f"task:e2e-task-1", ttl=30)
        assert lease1 is not None

        result1 = await completion_node.finalize(candidate1, TaskStatus.SUCCESS, result={"processed": 1})
        assert result1 is not None
        assert result1.status == TaskStatus.SUCCESS

        candidate2 = await queue_manager.dequeue()
        assert candidate2 is not None
        assert candidate2.id == "e2e-task-2"

        lease2 = await lock.acquire(f"task:e2e-task-2", ttl=5)
        assert lease2 is not None

        await asyncio.sleep(7)

        repaired = await reconciler.reconcile()
        assert repaired == 1

        candidate2_requeued = await queue_manager.dequeue()
        assert candidate2_requeued is not None
        assert candidate2_requeued.id == "e2e-task-2"

        lease3 = await lock.acquire(f"task:e2e-task-2", ttl=30)
        assert lease3 is not None

        result2 = await completion_node.finalize(candidate2_requeued, TaskStatus.SUCCESS, result={"processed": 2})
        assert result2 is not None
        assert result2.status == TaskStatus.SUCCESS

        assert await queue_manager.dequeue() is None
    finally:
        await client.flushdb()
        await client.aclose()


@pytest.mark.asyncio
async def test_live_redis_end_to_end_consumer_loop_with_duplicate_completion() -> None:
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

        reconciler = TaskReconciler(
            config=ReconciliationConfig(stuck_after_seconds=0, repair_strategy=RepairStrategy.REQUEUE),
            queue_manager=queue_manager,
            lock_backend=lock_a,
            worker_registry=workers,
        )

        await workers.register(WorkerInfo(worker_id="worker-a-e2e", name="a"))
        await workers.register(WorkerInfo(worker_id="worker-b-e2e", name="b"))

        task = make_task("e2e-duplicate", TaskPriority.HIGH, {"duplicate": True})
        await queue_manager.enqueue(task)

        candidate = await queue_manager.dequeue()
        assert candidate is not None

        lease_a, lease_b = await asyncio.gather(
            lock_a.acquire(f"task:e2e-duplicate", ttl=30),
            lock_b.acquire(f"task:e2e-duplicate", ttl=30),
        )
        assert (lease_a is None) != (lease_b is None)

        winner = node_a if lease_a is not None else node_b
        loser = node_b if lease_a is not None else node_a

        result_a, result_b = await asyncio.gather(
            winner.finalize(candidate, TaskStatus.SUCCESS, result={"winner": True}),
            loser.finalize(candidate, TaskStatus.SUCCESS, result={"loser": True}),
        )

        stored_result = result_a or result_b
        assert stored_result is not None
        assert stored_result.status == TaskStatus.SUCCESS
        assert stored_result.result == {"winner": True}

        repaired = await reconciler.reconcile()
        assert repaired == 0

        assert await queue_manager.dequeue() is None
    finally:
        await client.flushdb()
        await client.aclose()