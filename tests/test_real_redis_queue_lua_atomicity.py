from __future__ import annotations

from datetime import datetime

import pytest

from async_scheduler.backends.redis import RedisQueueBackend
from async_scheduler.core.models import Task, TaskPriority, TaskStatus


class EvalQueueFakeAsyncRedis:
    def __init__(self) -> None:
        self.lists: dict[str, list[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.sets: dict[str, set[str]] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.eval_calls: list[tuple[str, int, tuple[object, ...]]] = []

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

    async def delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self.lists:
                del self.lists[key]
                count += 1
            if key in self.zsets:
                del self.zsets[key]
                count += 1
            if key in self.sets:
                del self.sets[key]
                count += 1
            if key in self.hashes:
                del self.hashes[key]
                count += 1
        return count

    async def sadd(self, key: str, member: str) -> int:
        self.sets.setdefault(key, set()).add(member)
        return 1

    async def srem(self, key: str, member: str) -> int:
        if key not in self.sets:
            return 0
        if member in self.sets[key]:
            self.sets[key].remove(member)
            return 1
        return 0

    async def sismember(self, key: str, member: str) -> bool:
        if key not in self.sets:
            return False
        return member in self.sets[key]

    async def scard(self, key: str) -> int:
        return len(self.sets.get(key, set()))

    async def hset(self, key: str, field: str, value: str) -> int:
        self.hashes.setdefault(key, {})[field] = value
        return 1

    async def hget(self, key: str, field: str) -> str | None:
        if key not in self.hashes:
            return None
        return self.hashes[key].get(field)

    async def hdel(self, key: str, field: str) -> int:
        if key not in self.hashes:
            return 0
        if field in self.hashes[key]:
            del self.hashes[key][field]
            return 1
        return 0

    async def hexists(self, key: str, field: str) -> bool:
        if key not in self.hashes:
            return False
        return field in self.hashes[key]

    async def exists(self, key: str) -> int:
        return int(key in self.lists or key in self.zsets or key in self.sets or key in self.hashes)

    async def eval(self, script: str, numkeys: int, *args: object):
        self.eval_calls.append((script, numkeys, args))
        keys = [str(v) for v in args[:numkeys]]
        argv = [str(v) for v in args[numkeys:]]

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

        if "lrem" in script and "rpush" in script:
            old_key, new_key = keys
            task_id = argv[0]
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
            return 1

        if "lrem" in script:
            ready_key = keys[0]
            task_id = argv[0]
            return await self.lrem(ready_key, 1, task_id)

        if "zrem" in script and "srem" in script:
            delayed_key, scheduled_key = keys
            payload, task_id = argv
            removed = await self.zrem(delayed_key, payload)
            if removed:
                self.sets.setdefault(scheduled_key, set()).discard(task_id)
            return removed

        raise AssertionError(f"Unexpected script: {script}")

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        if key not in self.lists:
            return []
        lst = self.lists[key]
        if start < 0:
            start = len(lst) + start
        if stop < 0:
            stop = len(lst) + stop
        stop = min(stop, len(lst) - 1)
        return lst[start:stop+1]


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
async def test_promote_due_task_uses_eval_atomic_move_when_available() -> None:
    client = EvalQueueFakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)
    task = make_task("lua-promo", TaskPriority.HIGH)
    payload = backend._serialize_task(task)
    await client.zadd("async-scheduler:queue:delayed", {payload: 0})

    await backend._promote_due_tasks()

    assert client.eval_calls
    popped = await backend.dequeue()
    assert popped is not None
    assert popped.id == task.id


@pytest.mark.asyncio
async def test_update_priority_uses_eval_atomic_move_when_available() -> None:
    client = EvalQueueFakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)
    task = make_task("lua-reprio", TaskPriority.LOW)
    await backend.enqueue(task)

    updated = await backend.update_priority(task.id, TaskPriority.HIGH)

    assert updated is True
    assert any("rpush" in script and "lrem" in script for script, _, _ in client.eval_calls)
    popped = await backend.dequeue()
    assert popped is not None
    assert popped.id == task.id
    assert popped.priority == TaskPriority.HIGH


@pytest.mark.asyncio
async def test_cancel_uses_eval_remove_when_available() -> None:
    client = EvalQueueFakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)
    task = make_task("lua-cancel")
    await backend.enqueue(task)

    cancelled = await backend.cancel(task.id)

    assert cancelled is True
    assert client.eval_calls
    assert await backend.dequeue() is None
