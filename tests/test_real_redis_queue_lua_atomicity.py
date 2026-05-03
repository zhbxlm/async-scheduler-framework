from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from async_scheduler.backends.redis import RedisQueueBackend
from async_scheduler.core.models import Task, TaskPriority, TaskStatus
from tests.fake_redis import FullFakeAsyncRedis


class EvalQueueFakeAsyncRedis(FullFakeAsyncRedis):
    def __init__(self) -> None:
        super().__init__()
        self.eval_calls: list[tuple[str, int, tuple[object, ...]]] = []

    async def eval(self, script: str, numkeys: int, *args: object):
        self.eval_calls.append((script, numkeys, args))
        keys = [str(v) for v in args[:numkeys]]
        argv = [str(v) for v in args[numkeys:]]

        # QUEUE_PROMOTE_SCRIPT: zrem delayed + srem scheduled + rpush ready + hset task_data
        if "zrem" in script and "srem" in script and "rpush" in script and "hset" in script:
            delayed_key, scheduled_key, ready_key, task_data_key = keys
            payload, task_id = argv
            bucket = self.zsets.get(delayed_key, {})
            if payload not in bucket:
                return 0
            bucket.pop(payload, None)
            self.sets.setdefault(scheduled_key, set()).discard(task_id)
            self.lists.setdefault(ready_key, []).append(task_id)
            self.hashes.setdefault(task_data_key, {})[task_id] = payload
            return 1

        # QUEUE_REPRIORITIZE_SCRIPT: lrem old + rpush new + hset task_data
        if "lrem" in script and "rpush" in script:
            old_key, new_key = keys[0], keys[1]
            task_data_key = keys[2] if len(keys) > 2 else None
            task_id = argv[0]
            new_payload = argv[1] if len(argv) > 1 else None
            values = self.lists.get(old_key, [])
            if task_id not in values:
                return 0
            removed = False
            kept: list[str] = []
            for item in values:
                if item == task_id and not removed:
                    removed = True
                    continue
                kept.append(item)
            self.lists[old_key] = kept
            self.lists.setdefault(new_key, []).append(task_id)
            if task_data_key and new_payload:
                self.hashes.setdefault(task_data_key, {})[task_id] = new_payload
            return 1

        # QUEUE_REMOVE_READY_SCRIPT: lrem ready
        if "lrem" in script:
            ready_key = keys[0]
            task_id = argv[0]
            return await self.lrem(ready_key, 1, task_id)

        # QUEUE_REMOVE_DELAYED_SCRIPT: zrem delayed + srem scheduled
        if "zrem" in script and "srem" in script:
            delayed_key, scheduled_key = keys
            payload, task_id = argv
            removed = await self.zrem(delayed_key, payload)
            if removed:
                self.sets.setdefault(scheduled_key, set()).discard(task_id)
            return removed

        # Fallthrough: just execute and return 0 (e.g. DELAYED_PROMOTE_CAP_SCRIPT)
        return 0


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
async def test_promote_due_task_is_dequeued_after_scheduled_time() -> None:
    """Tasks enqueued with a past scheduled_at should be dequeued."""
    client = EvalQueueFakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)
    task = make_task("lua-promo", TaskPriority.HIGH)
    # Enqueue with a past scheduled_at so it is immediately "due"
    past = datetime.utcnow() - timedelta(seconds=10)
    await backend.enqueue(task, scheduled_at=past)

    # _promote_due_tasks should make it available for dequeue
    await backend._promote_due_tasks()

    popped = await backend.dequeue()
    assert popped is not None
    assert popped.id == task.id


@pytest.mark.asyncio
async def test_update_priority_changes_dequeue_order() -> None:
    """After update_priority, task should be dequeued with new priority reflected."""
    client = EvalQueueFakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)
    task = make_task("lua-reprio", TaskPriority.LOW)
    await backend.enqueue(task)

    updated = await backend.update_priority(task.id, TaskPriority.HIGH)

    assert updated is True
    popped = await backend.dequeue()
    assert popped is not None
    assert popped.id == task.id
    assert popped.priority == TaskPriority.HIGH


@pytest.mark.asyncio
async def test_cancel_prevents_dequeue() -> None:
    """Cancelled tasks must not be returned from dequeue."""
    client = EvalQueueFakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)
    task = make_task("lua-cancel")
    await backend.enqueue(task)

    cancelled = await backend.cancel(task.id)

    assert cancelled is True
    assert await backend.dequeue() is None
