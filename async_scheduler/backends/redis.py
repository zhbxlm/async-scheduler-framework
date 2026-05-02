"""Redis-backed backend implementations.

This module now supports a hybrid mode:
- real Redis client use where practical
- deterministic in-process stand-ins when no client is provided

That lets the repository preserve stable semantics while incrementally moving
from Redis-shaped implementations to real shared-state Redis behavior.
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from datetime import datetime, timedelta
from textwrap import dedent
from typing import Any

try:
    from redis.asyncio import Redis
except Exception:  # pragma: no cover
    Redis = None

from async_scheduler.backends.base import CompletionDedupBackend, LockBackend, LockHandle, QueueBackend
from async_scheduler.core.models import Task, TaskPriority, TaskStatus


class RedisCompletionDedupBackend(CompletionDedupBackend):
    def __init__(
        self,
        redis_url: str,
        namespace: str = "async-scheduler",
        client: Any | None = None,
        **_: Any,
    ) -> None:
        self._redis_url = redis_url
        self._namespace = namespace
        self._client = client or (Redis.from_url(redis_url, decode_responses=True) if Redis is not None else None)
        self._fallback_guard = asyncio.Lock()
        self._fallback_claims: dict[str, datetime | None] = {}
        self._client_supports_set = self._client is not None and hasattr(self._client, "set")

    async def claim_once(self, key: str, ttl_seconds: float | None = None) -> bool:
        namespaced = self._key(key)
        if self._client_supports_set:
            ttl = None if ttl_seconds is None else max(1, int(ttl_seconds))
            result = await self._client.set(namespaced, "1", ex=ttl, nx=True)
            return bool(result)

        async with self._fallback_guard:
            self._purge_expired()
            if namespaced in self._fallback_claims:
                return False
            expires_at = None if ttl_seconds is None else datetime.utcnow() + timedelta(seconds=ttl_seconds)
            self._fallback_claims[namespaced] = expires_at
            return True

    async def clear(self) -> None:
        if hasattr(self._client, "flushdb"):
            await self._client.flushdb()
            return
        async with self._fallback_guard:
            self._fallback_claims.clear()

    def _purge_expired(self) -> None:
        now = datetime.utcnow()
        for key, expiry in list(self._fallback_claims.items()):
            if expiry is not None and expiry <= now:
                del self._fallback_claims[key]

    def _key(self, key: str) -> str:
        return f"{self._namespace}:completion:{key}"


COMPARE_DELETE_SCRIPT = dedent(
    """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    end
    return 0
    """
).strip()

COMPARE_EXPIRE_SCRIPT = dedent(
    """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('expire', KEYS[1], tonumber(ARGV[2]))
    end
    return 0
    """
).strip()

QUEUE_PROMOTE_SCRIPT = dedent(
    """
    if redis.call('zrem', KEYS[1], ARGV[1]) == 1 then
        redis.call('srem', KEYS[2], ARGV[2])
        redis.call('rpush', KEYS[3], ARGV[2])
        redis.call('hset', KEYS[4], ARGV[2], ARGV[1])
        return 1
    end
    return 0
    """
).strip()

QUEUE_REPRIORITIZE_SCRIPT = dedent(
    """
    if redis.call('lrem', KEYS[1], 1, ARGV[1]) == 1 then
        redis.call('rpush', KEYS[2], ARGV[2])
        return 1
    end
    return 0
    """
).strip()

QUEUE_REMOVE_READY_SCRIPT = dedent(
    """
    return redis.call('lrem', KEYS[1], 1, ARGV[1])
    """
).strip()

QUEUE_REMOVE_DELAYED_SCRIPT = dedent(
    """
    if redis.call('zrem', KEYS[1], ARGV[1]) == 1 then
        redis.call('srem', KEYS[2], ARGV[2])
        return 1
    end
    return 0
    """
).strip()


class RedisLockBackend(LockBackend):
    def __init__(
        self,
        redis_url: str,
        lease_ttl_seconds: float = 30.0,
        heartbeat_interval_seconds: float = 10.0,
        namespace: str = "async-scheduler",
        client: Any | None = None,
        **_: Any,
    ) -> None:
        self._redis_url = redis_url
        self._lease_ttl_seconds = lease_ttl_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._namespace = namespace
        self._client = client or (Redis.from_url(redis_url, decode_responses=True) if Redis is not None else None)
        self._client_supports_basic_ops = self._client is not None and all(
            hasattr(self._client, attr) for attr in ("set", "get", "delete", "expire")
        )
        self._guard = asyncio.Lock()
        self._leases: dict[str, LockHandle] = {}
        self._token_counter = 0
        self._compare_delete_script = None
        self._compare_expire_script = None

    async def acquire(self, key: str, ttl: float | None = None, wait: float | None = None) -> LockHandle | None:
        effective_ttl = self._lease_ttl_seconds if ttl is None else ttl
        deadline = None if wait is None else asyncio.get_running_loop().time() + wait
        while True:
            handle = self._build_handle(key, effective_ttl)
            if self._client_supports_basic_ops:
                ttl_int = max(1, int(effective_ttl)) if effective_ttl is not None else None
                result = await self._client.set(self._redis_key(key), handle.token, ex=ttl_int, nx=True)
                if result:
                    return handle
            else:
                async with self._guard:
                    self._purge_if_expired(key)
                    if key not in self._leases:
                        self._leases[key] = handle
                        return handle
            if wait is None:
                return None
            if asyncio.get_running_loop().time() >= deadline:
                return None
            await asyncio.sleep(0.01)

    async def release(self, handle: LockHandle) -> bool:
        if self._client_supports_basic_ops:
            key = self._redis_key(handle.key)
            deleted = await self._compare_delete(key, handle.token)
            return bool(deleted)
        async with self._guard:
            self._purge_if_expired(handle.key)
            current = self._leases.get(handle.key)
            if current is None or current.token != handle.token:
                return False
            del self._leases[handle.key]
            return True

    async def extend(self, handle: LockHandle, ttl: float) -> bool:
        if self._client_supports_basic_ops:
            key = self._redis_key(handle.key)
            ttl_int = max(1, int(ttl))
            extended = await self._compare_expire(key, handle.token, ttl_int)
            if not extended:
                return False
            handle.expires_at = datetime.utcnow() + timedelta(seconds=ttl)
            return True
        async with self._guard:
            self._purge_if_expired(handle.key)
            current = self._leases.get(handle.key)
            if current is None or current.token != handle.token:
                return False
            new_expires_at = datetime.utcnow() + timedelta(seconds=ttl)
            current.expires_at = new_expires_at
            handle.expires_at = new_expires_at
            return True

    async def is_locked(self, key: str) -> bool:
        if self._client_supports_basic_ops:
            return (await self._client.get(self._redis_key(key))) is not None
        async with self._guard:
            self._purge_if_expired(key)
            return key in self._leases

    async def _compare_delete(self, key: str, token: str) -> int:
        if hasattr(self._client, "compare_delete"):
            return int(await self._client.compare_delete(key, token))
        if hasattr(self._client, "eval"):
            return int(await self._client.eval(COMPARE_DELETE_SCRIPT, 1, key, token))
        current = await self._client.get(key)
        if current != token:
            return 0
        current_after_check = await self._client.get(key)
        if current_after_check != token:
            return 0
        return int(await self._client.delete(key))

    async def _compare_expire(self, key: str, token: str, ttl_int: int) -> bool:
        if hasattr(self._client, "compare_expire"):
            return bool(await self._client.compare_expire(key, token, ttl_int))
        if hasattr(self._client, "eval"):
            return bool(await self._client.eval(COMPARE_EXPIRE_SCRIPT, 1, key, token, ttl_int))
        current = await self._client.get(key)
        if current != token:
            return False
        current_after_check = await self._client.get(key)
        if current_after_check != token:
            return False
        return bool(await self._client.expire(key, ttl_int))

    def _build_handle(self, key: str, ttl: float | None) -> LockHandle:
        self._token_counter += 1
        expires_at = None if ttl is None else datetime.utcnow() + timedelta(seconds=ttl)
        return LockHandle(key=key, token=f"{self._namespace}:{key}:{self._token_counter}", expires_at=expires_at)

    def _purge_if_expired(self, key: str) -> None:
        handle = self._leases.get(key)
        if handle is None or handle.expires_at is None:
            return
        if handle.expires_at <= datetime.utcnow():
            del self._leases[key]

    def _redis_key(self, key: str) -> str:
        return f"{self._namespace}:lock:{key}"


class RedisQueueBackend(QueueBackend):
    def __init__(
        self,
        redis_url: str,
        lease_ttl_seconds: float = 30.0,
        heartbeat_interval_seconds: float = 10.0,
        namespace: str = "async-scheduler",
        client: Any | None = None,
        **_: Any,
    ) -> None:
        self._redis_url = redis_url
        self._lease_ttl_seconds = lease_ttl_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._namespace = namespace
        self._client = client or (Redis.from_url(redis_url, decode_responses=True) if Redis is not None else None)
        self._client_supports_queue_ops = self._client is not None and all(
            hasattr(self._client, attr) for attr in ("rpush", "lpop", "llen", "zadd", "zrangebyscore", "zrem", "delete", "exists", "scard", "sadd", "srem", "sismember")
        )
        self._lock = asyncio.Lock()
        self._cancelled_set_key = f"{namespace}:queue:cancelled"
        self._task_data_key = f"{namespace}:queue:task_data"
        self._scheduled_set_key = f"{namespace}:queue:scheduled_ids"
        self._queue_counts: defaultdict[int, int] = defaultdict(int)
        self._scheduled_ids: set[str] = set()
        self._task_index: dict[str, Task] = {}
        self._cancelled_ids: set[str] = set()
        self._ready_store: defaultdict[int, list[str]] = defaultdict(list)
        self._delayed_store: dict[str, float] = {}


    async def enqueue(self, task: Task, scheduled_at: datetime | None = None) -> None:
        serialized = self._serialize_task(task)
        if self._client_supports_queue_ops:
            await self._client.srem(self._cancelled_set_key, task.id)
            if scheduled_at and scheduled_at > datetime.utcnow():
                await self._client.zadd(self._delayed_key(), {serialized: scheduled_at.timestamp()})
                await self._client.sadd(self._scheduled_set_key, task.id)
                return
            await self._client.hset(self._task_data_key, task.id, serialized)
            await self._client.rpush(self._ready_key(task.priority.value), task.id)
            return
        if not self._client_supports_queue_ops:
            async with self._lock:
                await self._push_ready(serialized, task.priority.value, scheduled_at)

    async def dequeue(self, timeout: float | None = None) -> Task | None:
        await self._promote_due_tasks()
        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
        while True:
            if self._client_supports_queue_ops:
                for priority in sorted(TaskPriority, key=lambda p: p.value, reverse=True):
                    task_id = await self._client.lpop(self._ready_key(priority.value))
                    if task_id is None:
                        continue
                    is_cancelled = await self._client.sismember(self._cancelled_set_key, task_id)
                    if is_cancelled:
                        await self._client.srem(self._cancelled_set_key, task_id)
                        await self._client.hdel(self._task_data_key, task_id)
                        continue
                    payload = await self._client.hget(self._task_data_key, task_id)
                    if not payload:
                        continue
                    await self._client.hdel(self._task_data_key, task_id)
                    return self._deserialize_task(payload)
            else:
                for priority in sorted(TaskPriority, key=lambda p: p.value, reverse=True):
                    queue = self._ready_store[priority.value]
                    payload = queue.pop(0) if queue else None
                    if payload is None:
                        continue
                    task = self._deserialize_task(payload)
                    if task.id in self._cancelled_ids:
                        self._cancelled_ids.discard(task.id)
                        self._task_index.pop(task.id, None)
                        continue
                    self._queue_counts[priority.value] = max(0, self._queue_counts[priority.value] - 1)
                    self._task_index.pop(task.id, None)
                    return task
            if timeout is None or asyncio.get_running_loop().time() >= deadline:
                return None
            await asyncio.sleep(0.01)
            await self._promote_due_tasks()

    async def peek(self, limit: int = 10) -> list[Task]:
        await self._promote_due_tasks()
        if self._client_supports_queue_ops:
            tasks: list[Task] = []
            for priority in sorted(TaskPriority, key=lambda p: p.value, reverse=True):
                task_ids = await self._client.lrange(self._ready_key(priority.value), 0, limit - len(tasks) - 1)
                for task_id in task_ids:
                    is_cancelled = await self._client.sismember(self._cancelled_set_key, task_id)
                    if is_cancelled:
                        continue
                    payload = await self._client.hget(self._task_data_key, task_id)
                    if payload:
                        tasks.append(self._deserialize_task(payload))
                        if len(tasks) >= limit:
                            return tasks
            return tasks
        tasks: list[Task] = []
        for priority in sorted(TaskPriority, key=lambda p: p.value, reverse=True):
            values = self._ready_store[priority.value][:limit]
            for payload in values:
                task = self._deserialize_task(payload)
                if task.id in self._cancelled_ids:
                    continue
                tasks.append(task)
                if len(tasks) >= limit:
                    return tasks
        return tasks

    async def cancel(self, task_id: str) -> bool:
        if self._client_supports_queue_ops:
            await self._client.sadd(self._cancelled_set_key, task_id)
            task_exists = await self._client.hexists(self._task_data_key, task_id)
            if not task_exists:
                return False
            payload = await self._client.hget(self._task_data_key, task_id)
            if not payload:
                return False
            task = self._deserialize_task(payload)
            removed = 0
            if await self._client.sismember(self._scheduled_set_key, task_id):
                removed = await self._remove_delayed_atomic(payload)
            else:
                removed = await self._remove_ready_atomic(task.priority.value, task_id)
            return bool(removed)
        async with self._lock:
            task = self._task_index.get(task_id)
            if task is None:
                return False
            self._cancelled_ids.add(task_id)
            if task_id in self._scheduled_ids:
                self._scheduled_ids.remove(task_id)
                self._remove_from_delayed(task_id)
            else:
                self._remove_from_ready(task_id)
            self._task_index.pop(task_id, None)
            return True

    async def update_priority(self, task_id: str, new_priority: TaskPriority) -> bool:
        if self._client_supports_queue_ops:
            is_scheduled = await self._client.sismember(self._scheduled_set_key, task_id)
            if is_scheduled:
                return False
            payload = await self._client.hget(self._task_data_key, task_id)
            if not payload:
                return False
            task = self._deserialize_task(payload)
            original_priority = task.priority
            task.priority = new_priority
            new_payload = self._serialize_task(task)
            moved = await self._reprioritize_atomic(original_priority.value, new_priority.value, task_id, new_payload)
            if not moved:
                return False
            await self._client.hset(self._task_data_key, task_id, new_payload)
            return True
        async with self._lock:
            task = self._task_index.get(task_id)
            if task is None or task_id in self._scheduled_ids:
                return False
            self._remove_from_ready(task_id)
            task.priority = new_priority
            await self._push_ready(self._serialize_task(task), new_priority.value)
            return True

    async def size(self) -> dict[int, int]:
        if self._client_supports_queue_ops:
            return {
                priority.value: await self._client.llen(self._ready_key(priority.value))
                for priority in TaskPriority
            }
        return {priority.value: self._queue_counts[priority.value] for priority in TaskPriority}

    async def clear(self) -> None:
        if self._client_supports_queue_ops:
            keys = [self._ready_key(priority.value) for priority in TaskPriority]
            keys.append(self._delayed_key())
            keys.append(self._cancelled_set_key)
            keys.append(self._task_data_key)
            keys.append(self._scheduled_set_key)
            await self._client.delete(*keys)
            return
        async with self._lock:
            for priority in TaskPriority:
                self._ready_store[priority.value].clear()
                self._queue_counts[priority.value] = 0
            self._delayed_store.clear()
            self._scheduled_ids.clear()
            self._task_index.clear()
            self._cancelled_ids.clear()

    async def is_scheduled(self, task_id: str) -> bool:
        if self._client_supports_queue_ops:
            return await self._client.sismember(self._scheduled_set_key, task_id)
        return task_id in self._scheduled_ids

    async def get_scheduled_count(self) -> int:
        if self._client_supports_queue_ops:
            return await self._client.scard(self._scheduled_set_key)
        return len(self._scheduled_ids)

    async def get_queue_count(self) -> int:
        if self._client_supports_queue_ops:
            total = 0
            for priority in TaskPriority:
                total += await self._client.llen(self._ready_key(priority.value))
            return total
        return sum(self._queue_counts.values())

    async def _push_ready(self, serialized: str, priority: int, scheduled_at: datetime | None = None) -> None:
        if scheduled_at and scheduled_at > datetime.utcnow():
            self._delayed_store[serialized] = scheduled_at.timestamp()
            task = self._deserialize_task(serialized)
            self._scheduled_ids.add(task.id)
            self._task_index[task.id] = task
            return
        self._ready_store[priority].append(serialized)
        task = self._deserialize_task(serialized)
        task.status = TaskStatus.QUEUED
        self._task_index[task.id] = task
        self._queue_counts[priority] += 1

    async def _promote_due_tasks(self) -> None:
        now = datetime.utcnow().timestamp()
        if self._client_supports_queue_ops:
            due_payloads = await self._client.zrangebyscore(self._delayed_key(), float("-inf"), now)
            for payload in due_payloads:
                task = self._deserialize_task(payload)
                moved = await self._promote_due_atomic(task.priority.value, payload)
                if not moved:
                    continue
            return
        if not self._client_supports_queue_ops:
            due_payloads = [payload for payload, score in self._delayed_store.items() if score <= now]
            if not due_payloads:
                return
            async with self._lock:
                for payload in due_payloads:
                    if payload not in self._delayed_store:
                        continue
                    del self._delayed_store[payload]
                    task = self._deserialize_task(payload)
                    self._scheduled_ids.discard(task.id)
                    await self._push_ready(payload, task.priority.value)

    def _remove_from_delayed(self, task_id: str) -> None:
        for payload in list(self._delayed_store.keys()):
            task = self._deserialize_task(payload)
            if task.id == task_id:
                del self._delayed_store[payload]
                return

    def _remove_from_ready(self, task_id: str) -> None:
        for priority in TaskPriority:
            queue = self._ready_store[priority.value]
            for idx, payload in enumerate(list(queue)):
                task = self._deserialize_task(payload)
                if task.id == task_id:
                    del queue[idx]
                    self._queue_counts[priority.value] = max(0, self._queue_counts[priority.value] - 1)
                    return

    async def _promote_due_atomic(self, priority: int, payload: str) -> int:
        delayed_key = self._delayed_key()
        scheduled_key = self._scheduled_set_key
        ready_key = self._ready_key(priority)
        task = self._deserialize_task(payload)
        if hasattr(self._client, "eval"):
            return int(await self._client.eval(QUEUE_PROMOTE_SCRIPT, 4, delayed_key, scheduled_key, ready_key, self._task_data_key, payload, task.id))
        removed = await self._client.zrem(delayed_key, payload)
        if not removed:
            return 0
        await self._client.srem(scheduled_key, task.id)
        await self._client.rpush(ready_key, task.id)
        await self._client.hset(self._task_data_key, task.id, payload)
        return 1

    async def _reprioritize_atomic(self, old_priority: int, new_priority: int, task_id: str, new_payload: str) -> int:
        old_key = self._ready_key(old_priority)
        new_key = self._ready_key(new_priority)
        if hasattr(self._client, "eval"):
            return int(await self._client.eval(QUEUE_REPRIORITIZE_SCRIPT, 2, old_key, new_key, task_id))
        removed = await self._client.lrem(old_key, 1, task_id)
        if not removed:
            return 0
        await self._client.rpush(new_key, task_id)
        await self._client.hset(self._task_data_key, task_id, new_payload)
        return 1

    async def _remove_ready_atomic(self, priority: int, task_id: str) -> int:
        ready_key = self._ready_key(priority)
        if hasattr(self._client, "eval"):
            return int(await self._client.eval(QUEUE_REMOVE_READY_SCRIPT, 1, ready_key, task_id))
        return int(await self._client.lrem(ready_key, 1, task_id))

    async def _remove_delayed_atomic(self, payload: str) -> int:
        delayed_key = self._delayed_key()
        scheduled_key = self._scheduled_set_key
        task = self._deserialize_task(payload)
        if hasattr(self._client, "eval"):
            return int(await self._client.eval(QUEUE_REMOVE_DELAYED_SCRIPT, 2, delayed_key, scheduled_key, payload, task.id))
        removed = await self._client.zrem(delayed_key, payload)
        if not removed:
            return 0
        await self._client.srem(scheduled_key, task.id)
        return 1

    def _serialize_task(self, task: Task) -> str:
        return json.dumps(task.model_dump(mode="json"), sort_keys=True)

    def _deserialize_task(self, payload: str) -> Task:
        return Task.model_validate(json.loads(payload))

    def _ready_key(self, priority: int) -> str:
        return f"{self._namespace}:queue:ready:{priority}"

    def _delayed_key(self) -> str:
        return f"{self._namespace}:queue:delayed"
