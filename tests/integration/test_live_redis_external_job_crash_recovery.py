from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta

import pytest

redis_asyncio = pytest.importorskip("redis.asyncio")

from async_scheduler.backends.redis import RedisCompletionDedupBackend, RedisLockBackend, RedisQueueBackend  # noqa: E402
from async_scheduler.core.models import Task, TaskStatus  # noqa: E402
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
async def test_live_redis_external_job_crash_and_recovery_with_new_worker() -> None:
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

        await workers.register(WorkerInfo(worker_id="worker-original", name="original", heartbeat_ttl=3))

        task = make_task("external-job-crash", -1)
        await queue_manager.enqueue(task)

        candidate = await queue_manager.dequeue()
        assert candidate is not None

        lease = await lock.acquire(f"task:{task.id}", ttl=3)
        assert lease is not None

        await asyncio.sleep(4)

        await workers.unregister("worker-original")
        await workers.register(WorkerInfo(worker_id="worker-recovery", name="recovery", heartbeat_ttl=10))

        repaired = await reconciler.reconcile()
        assert repaired == 1

        candidate = await queue_manager.dequeue()
        assert candidate is not None

        lease_recovery = await lock.acquire(f"task:{task.id}", ttl=30)
        assert lease_recovery is not None

        result = await completion_node.finalize(candidate, TaskStatus.SUCCESS, result={"recovered": "external"})
        assert result is not None
        assert result.status == TaskStatus.SUCCESS
        assert result.result == {"recovered": "external"}
        assert await queue_manager.dequeue() is None
    finally:
        await client.flushdb()
        await client.aclose()


@pytest.mark.asyncio
async def test_live_redis_external_job_crash_before_heartbeat_without_registry() -> None:
    client = await _build_live_client(LIVE_REDIS_URL)
    try:
        await client.flushdb()

        queue = RedisQueueBackend(redis_url=LIVE_REDIS_URL, client=client)
        queue_manager = QueueManager(queue)
        lock = RedisLockBackend(redis_url=LIVE_REDIS_URL, client=client)
        dedup = RedisCompletionDedupBackend(redis_url=LIVE_REDIS_URL, client=client)
        completion_node = TaskCompletionNode(dedup_backend=dedup)

        task = make_task("external-crash-no-registry", -1)
        await queue_manager.enqueue(task)

        candidate = await queue_manager.dequeue()
        assert candidate is not None

        lease = await lock.acquire(f"task:{task.id}", ttl=2)
        assert lease is not None

        await asyncio.sleep(3)

        candidate = await queue_manager.dequeue()
        assert candidate is not None

        lease_recovery = await lock.acquire(f"task:{task.id}", ttl=30)
        assert lease_recovery is not None

        result = await completion_node.finalize(candidate, TaskStatus.SUCCESS, result={"recovered": "no-registry"})
        assert result is not None
        assert result.status == TaskStatus.SUCCESS
        assert result.result == {"recovered": "no-registry"}
        assert await queue_manager.dequeue() is None
    finally:
        await client.flushdb()
        await client.aclose()


@pytest.mark.asyncio
async def test_live_redis_external_job_partial_work_and_abandoned_lease() -> None:
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

        await workers.register(WorkerInfo(worker_id="worker-partial", name="partial", heartbeat_ttl=2))

        task = make_task("partial-work-abandoned", 0)
        await queue_manager.enqueue(task)

        candidate = await queue_manager.dequeue()
        assert candidate is not None

        lease = await lock.acquire(f"task:{task.id}", ttl=2)
        assert lease is not None

        await asyncio.sleep(1)

        repaired = await reconciler.reconcile()
        assert repaired == 0

        await asyncio.sleep(2)

        repaired = await reconciler.reconcile()
        assert repaired == 1

        candidate = await queue_manager.dequeue()
        assert candidate is not None

        lease_recovery = await lock.acquire(f"task:{task.id}", ttl=30)
        assert lease_recovery is not None

        result = await completion_node.finalize(candidate, TaskStatus.SUCCESS, result={"finished": "partial"})
        assert result is not None
        assert result.status == TaskStatus.SUCCESS
        assert result.result == {"finished": "partial"}
        assert await queue_manager.dequeue() is None
    finally:
        await client.flushdb()
        await client.aclose()