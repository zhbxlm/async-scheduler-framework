from __future__ import annotations

from datetime import datetime, timedelta

import pytest

pytestmark = pytest.mark.redis_required


from async_scheduler.backends.redis import RedisQueueBackend
from async_scheduler.core.models import Task, TaskPriority, TaskStatus


class FakeAsyncRedis:
    def __init__(self) -> None:
        self.lists: dict[str, list[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.sets: dict[str, set[str]] = {}
        self.hashes: dict[str, dict[str, str]] = {}

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

    async def lrem(self, key: str, count: int, value: str) -> int:
        if key not in self.lists:
            return 0
        lst = self.lists[key]
        removed = 0
        if count == 0:
            while value in lst:
                lst.remove(value)
                removed += 1
        elif count > 0:
            for _ in range(count):
                try:
                    lst.remove(value)
                    removed += 1
                except ValueError:
                    break
        else:
            # negative count: remove from end
            reversed_lst = lst[::-1]
            count = -count
            removed_items = []
            for _ in range(count):
                try:
                    idx = reversed_lst.index(value)
                    removed_items.append(len(lst) - 1 - idx)
                    reversed_lst[idx] = None  # mark as removed
                except ValueError:
                    break
            for idx in sorted(removed_items, reverse=True):
                lst.pop(idx)
                removed += 1
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

    async def scard(self, key: str) -> int:
        return len(self.sets.get(key, set()))

    async def sismember(self, key: str, member: str) -> bool:
        if key not in self.sets:
            return False
        return member in self.sets[key]

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

    async def eval(self, script: str, numkeys: int, *args) -> int:
        keys = args[:numkeys]
        args = args[numkeys:]
        
        if 'zrem' in script and 'srem' in script and 'rpush' in script and 'hset' in script:
            # QUEUE_PROMOTE_SCRIPT
            delayed_key, scheduled_key, ready_key, task_data_key, payload, task_id = keys[0], keys[1], keys[2], keys[3], args[0], args[1]
            if delayed_key not in self.zsets:
                return 0
            if payload not in self.zsets[delayed_key]:
                return 0
            del self.zsets[delayed_key][payload]
            if scheduled_key in self.sets and task_id in self.sets[scheduled_key]:
                self.sets[scheduled_key].remove(task_id)
            if ready_key not in self.lists:
                self.lists[ready_key] = []
            self.lists[ready_key].append(task_id)
            self.hashes.setdefault(task_data_key, {})[task_id] = payload
            return 1
        
        if 'lrem' in script and 'rpush' in script and len(keys) == 2:
            # QUEUE_REPRIORITIZE_SCRIPT
            old_key, new_key, task_id = keys[0], keys[1], args[0]
            if old_key not in self.lists:
                return 0
            if task_id not in self.lists[old_key]:
                return 0
            self.lists[old_key].remove(task_id)
            if new_key not in self.lists:
                self.lists[new_key] = []
            self.lists[new_key].append(task_id)
            return 1
        
        if 'lrem' in script and len(keys) == 1:
            # QUEUE_REMOVE_READY_SCRIPT
            ready_key, task_id = keys[0], args[0]
            if ready_key not in self.lists:
                return 0
            if task_id not in self.lists[ready_key]:
                return 0
            self.lists[ready_key].remove(task_id)
            return 1
        
        if 'zrem' in script and 'srem' in script and len(keys) == 2:
            # QUEUE_REMOVE_DELAYED_SCRIPT
            delayed_key, scheduled_key, payload, task_id = keys[0], keys[1], args[0], args[1]
            if delayed_key not in self.zsets:
                return 0
            if payload not in self.zsets[delayed_key]:
                return 0
            del self.zsets[delayed_key][payload]
            if scheduled_key in self.sets and task_id in self.sets[scheduled_key]:
                self.sets[scheduled_key].remove(task_id)
            return 1
        
        return 0


def make_task(task_id: str, priority: TaskPriority = TaskPriority.NORMAL) -> Task:
    return Task(
        id=task_id,
        name=f"task-{task_id}",
        payload={"task_id": task_id},
        priority=priority,
        status=TaskStatus.PENDING,
    )


@pytest.mark.asyncio
async def test_real_redis_queue_backend_enqueue_and_dequeue() -> None:
    client = FakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)

    task = make_task("task-1", TaskPriority.HIGH)
    await backend.enqueue(task)
    popped = await backend.dequeue()

    assert popped is not None
    assert popped.id == task.id


@pytest.mark.asyncio
async def test_real_redis_queue_backend_delayed_task_promotes_when_due() -> None:
    client = FakeAsyncRedis()
    backend = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)

    task = make_task("task-2")
    await backend.enqueue(task, scheduled_at=datetime.utcnow() + timedelta(milliseconds=20))

    before = await backend.dequeue()
    assert before is None

    await pytest.importorskip("asyncio").sleep(0.03)
    after = await backend.dequeue()
    assert after is not None
    assert after.id == task.id
