from __future__ import annotations

import os
from datetime import datetime, timedelta

import pytest

redis_asyncio = pytest.importorskip("redis.asyncio")

from async_scheduler.backends.redis import RedisCompletionDedupBackend, RedisLockBackend, RedisQueueBackend  # noqa: E402
from async_scheduler.core.models import (  # noqa: E402
    ExecutionAttemptCreate,
    ExecutionAttemptStatus,
    Task,
    TaskCreate,
    TaskStatus,
)
from async_scheduler.distributed.worker_registry import WorkerInfo, WorkerRegistry  # noqa: E402
from async_scheduler.persistence import (  # noqa: E402
    ExecutionAttemptRepository,
    TaskRepository,
    drop_db,
    get_session,
    get_session_no_context,
    init_db,
)
from async_scheduler.platform.completion import TaskCompletionNode  # noqa: E402
from async_scheduler.platform.reconciler import (  # noqa: E402
    ReconciliationConfig,
    RepairStrategy,
    TaskReconciler,
)
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


@pytest.mark.asyncio
async def test_live_redis_success_completion_prevents_reconciler_requeue_overlap() -> None:
    client = await _build_live_client(LIVE_REDIS_URL)
    try:
        await client.flushdb()
        await drop_db()
        await init_db()

        queue = RedisQueueBackend(redis_url=LIVE_REDIS_URL, client=client)
        queue_manager = QueueManager(queue)
        lock_backend = RedisLockBackend(redis_url=LIVE_REDIS_URL, client=client)
        worker_registry = WorkerRegistry(
            redis_url=LIVE_REDIS_URL,
            heartbeat_ttl_seconds=30.0,
            client=client,
        )
        completion = TaskCompletionNode(
            dedup_backend=RedisCompletionDedupBackend(redis_url=LIVE_REDIS_URL, client=client)
        )
        reconciler = TaskReconciler(
            config=ReconciliationConfig(stuck_after_seconds=0, repair_strategy=RepairStrategy.REQUEUE),
            queue_manager=queue_manager,
            lock_backend=lock_backend,
            worker_registry=worker_registry,
        )

        async with get_session() as session:
            task = await TaskRepository.create(session, TaskCreate(name="live-overlap", payload={}))
            await TaskRepository.update(
                session,
                task.id,
                status=TaskStatus.RUNNING,
                started_at=datetime.utcnow() - timedelta(seconds=5),
                updated_at=datetime.utcnow() - timedelta(seconds=5),
            )
            attempt = await ExecutionAttemptRepository.create(
                session,
                ExecutionAttemptCreate(
                    task_id=task.id,
                    worker_id="worker-live-overlap",
                    retry_index=0,
                    lease_token="lease-live-overlap",
                ),
            )
            await ExecutionAttemptRepository.update(
                session,
                attempt.id,
                status=ExecutionAttemptStatus.RUNNING,
                started_at=datetime.utcnow() - timedelta(seconds=5),
                last_heartbeat_at=datetime.utcnow(),
            )
            stored = await TaskRepository.get(session, task.id)
            assert stored is not None
            runtime_task = Task.model_validate(stored.model_dump())

        await worker_registry.register(WorkerInfo(worker_id="worker-live-overlap", name="overlap"))
        lease = await lock_backend.acquire(f"task:{task.id}", ttl=30)
        assert lease is not None

        updated = await completion.finalize(
            runtime_task,
            TaskStatus.SUCCESS,
            result={"ok": True},
            finalize_latest_attempt=True,
        )
        assert updated is not None

        repaired = await reconciler.reconcile()

        async with await get_session_no_context() as session:
            final_task = await TaskRepository.get(session, task.id)
            final_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

        assert repaired == 0
        assert final_task is not None
        assert final_task.status == TaskStatus.SUCCESS
        assert final_task.result == {"ok": True}
        assert final_attempt is not None
        assert final_attempt.status == ExecutionAttemptStatus.SUCCEEDED
        assert await queue_manager.dequeue() is None
        assert await lock_backend.release(lease) is True
    finally:
        await client.flushdb()
        await client.aclose()
