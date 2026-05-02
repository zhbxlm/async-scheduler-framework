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
import time
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

# New Lua scripts for capability-aware queue
CAPABILITY_ENQUEUE_SCRIPT = dedent(
    """
    -- KEYS[1] = pending_key, KEYS[2] = stats_key, KEYS[3] = registry_key, KEYS[4] = task_data_key
    -- ARGV[1] = task_id, ARGV[2] = payload, ARGV[3] = score, ARGV[4] = capability
    local pending_key = KEYS[1]
    local stats_key = KEYS[2]
    local registry_key = KEYS[3]
    local task_data_key = KEYS[4]
    
    -- Add to pending ZSET
    redis.call('zadd', pending_key, ARGV[3], ARGV[1])
    -- Increment enqueue count
    redis.call('hincrby', stats_key, 'enqueue_count', 1)
    -- Register capability
    redis.call('sadd', registry_key, ARGV[4])
    -- Store task data
    redis.call('hset', task_data_key, ARGV[1], ARGV[2])
    return 1
    """
).strip()

CAPABILITY_DEQUEUE_SCRIPT = dedent(
    """
    -- KEYS[1] = pending_key, KEYS[2] = running_key, KEYS[3] = stats_key, KEYS[4] = config_key, KEYS[5] = task_data_key
    -- ARGV[1] = current_time_ms, ARGV[2] = capability
    local pending_key = KEYS[1]
    local running_key = KEYS[2]
    local stats_key = KEYS[3]
    local config_key = KEYS[4]
    local task_data_key = KEYS[5]
    
    -- Check max_concurrent
    local max_concurrent = tonumber(redis.call('hget', config_key, 'max_concurrent'))
    if max_concurrent ~= nil then
        local running_count = redis.call('zcard', running_key)
        if running_count >= max_concurrent then
            return nil
        end
    end
    
    -- Pop highest priority (lowest score) task
    local result = redis.call('zpopmin', pending_key, 1)
    if #result == 0 then
        return nil
    end
    
    local task_id = result[1]
    local score = tonumber(result[2])
    
    -- Move to running set with current timestamp as score
    redis.call('zadd', running_key, ARGV[1], task_id)
    
    -- Increment dequeue count
    redis.call('hincrby', stats_key, 'dequeue_count', 1)
    
    -- Get task payload
    local payload = redis.call('hget', task_data_key, task_id)
    if payload == false then
        -- Remove from running if no task data
        redis.call('zrem', running_key, task_id)
        return nil
    end
    
    return {task_id, payload}
    """
).strip()

CAPABILITY_COMPLETE_SCRIPT = dedent(
    """
    -- KEYS[1] = running_key, KEYS[2] = stats_key, KEYS[3] = task_data_key
    -- ARGV[1] = task_id
    local running_key = KEYS[1]
    local stats_key = KEYS[2]
    local task_data_key = KEYS[3]
    
    -- Remove from running
    local removed = redis.call('zrem', running_key, ARGV[1])
    if removed == 1 then
        -- Increment complete count
        redis.call('hincrby', stats_key, 'complete_count', 1)
        -- Clean task data (optional)
        redis.call('hdel', task_data_key, ARGV[1])
        return 1
    end
    return 0
    """
).strip()

CAPABILITY_FAIL_SCRIPT = dedent(
    """
    -- KEYS[1] = running_key, KEYS[2] = stats_key, KEYS[3] = task_data_key
    -- ARGV[1] = task_id
    local running_key = KEYS[1]
    local stats_key = KEYS[2]
    local task_data_key = KEYS[3]
    
    -- Remove from running
    local removed = redis.call('zrem', running_key, ARGV[1])
    if removed == 1 then
        -- Increment fail count
        redis.call('hincrby', stats_key, 'fail_count', 1)
        -- Clean task data (optional)
        redis.call('hdel', task_data_key, ARGV[1])
        return 1
    end
    return 0
    """
).strip()

DELAYED_PROMOTE_SCRIPT = dedent(
    """
    -- KEYS[1] = delayed_key, KEYS[2] = pending_key, KEYS[3] = stats_key, KEYS[4] = registry_key, KEYS[5] = task_data_key, KEYS[6] = scheduled_set_key
    -- ARGV[1] = current_time, ARGV[2] = capability
    local delayed_key = KEYS[1]
    local pending_key = KEYS[2]
    local stats_key = KEYS[3]
    local registry_key = KEYS[4]
    local task_data_key = KEYS[5]
    local scheduled_set_key = KEYS[6]
    
    -- Get due tasks
    local due_tasks = redis.call('zrangebyscore', delayed_key, 0, ARGV[1], 'WITHSCORES')
    local promoted = 0
    
    for i = 1, #due_tasks, 2 do
        local payload = due_tasks[i]
        local scheduled_at = tonumber(due_tasks[i+1])
        
        -- Parse task id from payload (first 36 chars for UUID)
        local task_id = payload:sub(1, 36)
        
        -- Move to pending
        redis.call('zadd', pending_key, scheduled_at * 1000, task_id)  -- Use scheduled_at as score
        redis.call('sadd', registry_key, ARGV[2])
        redis.call('hset', task_data_key, task_id, payload)
        
        -- Remove from delayed and scheduled set
        redis.call('zrem', delayed_key, payload)
        redis.call('srem', scheduled_set_key, task_id)
        
        promoted = promoted + 1
    end
    
    return promoted
    """
).strip()

# G6: Lua script for atomic time-gated promotion of delayed tasks
# Reads delayed ZSET by score <= now, parses wrapped payload, promotes to
# capability pending ZSET and updates task_data hash atomically.
DELAYED_PROMOTE_CAP_SCRIPT = dedent(
    """
    -- KEYS[1] = delayed_key (global delayed ZSET)
    -- KEYS[2] = cap_registry_key (capabilities:registry Set)
    -- KEYS[3] = scheduled_set_key
    -- ARGV[1] = now_ts (unix seconds float as string)
    -- ARGV[2] = ts_ms (current timestamp in ms)
    -- ARGV[3] = capability filter ("*" = all, else exact match)
    local delayed_key      = KEYS[1]
    local cap_registry_key = KEYS[2]
    local scheduled_set    = KEYS[3]
    local now_ts           = tonumber(ARGV[1])
    local ts_ms            = tonumber(ARGV[2])
    local cap_filter       = ARGV[3]

    -- Get all tasks with scheduled_at <= now
    local due = redis.call('ZRANGEBYSCORE', delayed_key, '-inf', now_ts, 'WITHSCORES')
    local promoted = 0

    local i = 1
    while i <= #due do
        local payload = due[i]
        -- expected format: JSON with {"capability": "...", "task": {...}}
        -- extract capability via simple string search (no cjson on all Redis versions)
        local cap_start = payload:find('"capability":') 
        local cap = 'default'
        if cap_start then
            local val_start = payload:find('"', cap_start + 13) + 1
            local val_end   = payload:find('"', val_start) - 1
            if val_start and val_end then
                cap = payload:sub(val_start, val_end)
            end
        end

        -- Apply capability filter
        if cap_filter == '*' or cap_filter == cap then
            -- Extract task_id: find "id":"..."
            local id_start = payload:find('"id":"')
            local task_id  = ''
            if id_start then
                local vs = payload:find('"', id_start + 6) + 1
                local ve = payload:find('"', vs) - 1
                if vs and ve then
                    task_id = payload:sub(vs, ve)
                end
            end

            if task_id ~= '' then
                -- Build capability-specific keys (namespace prefix from task data)
                -- We pass pending_key and task_data_key as dynamic from ARGV to keep script generic.
                -- Instead, the caller passes namespace prefix in ARGV[4] so we can build keys.
                local ns = ARGV[4] or 'async-scheduler'
                local pending_key   = ns .. ':' .. cap .. ':pending'
                local task_data_key = ns .. ':' .. cap .. ':task_data'
                local stats_key     = ns .. ':' .. cap .. ':stats'

                -- Decode priority_rank from task JSON (default 3 = NORMAL)
                local pri_rank = 3
                local pr_start = payload:find('"priority":')
                if pr_start then
                    local pr_num = payload:match('"priority": *([0-9]+)', pr_start)
                    if pr_num then pri_rank = tonumber(pr_num) end
                end

                -- score = priority_rank * 10^13 + ts_ms
                local score = pri_rank * 10000000000000 + ts_ms

                redis.call('HSET', task_data_key, task_id, payload)
                redis.call('ZADD', pending_key, score, task_id)
                redis.call('SADD', cap_registry_key, cap)
                redis.call('HINCRBY', stats_key, 'promote_count', 1)
                redis.call('ZREM', delayed_key, payload)
                redis.call('SREM', scheduled_set, task_id)

                promoted = promoted + 1
            end
        end

        i = i + 2
    end

    return promoted
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

    async def describe_lock(self, key: str) -> dict[str, Any]:
        redis_key = self._redis_key(key)
        if self._client_supports_basic_ops:
            token = await self._client.get(redis_key)
            ttl_ms = None
            if token is not None and hasattr(self._client, "pttl"):
                ttl_ms = await self._client.pttl(redis_key)
            return {
                "key": key,
                "backend": "redis",
                "redis_key": redis_key,
                "locked": token is not None,
                "token": token,
                "ttl_ms": None if ttl_ms is None or ttl_ms < 0 else int(ttl_ms),
                "lease_ttl_seconds": self._lease_ttl_seconds,
                "heartbeat_interval_seconds": self._heartbeat_interval_seconds,
            }
        async with self._guard:
            self._purge_if_expired(key)
            handle = self._leases.get(key)
            ttl_ms = None
            if handle is not None and handle.expires_at is not None:
                ttl_ms = max(0, int((handle.expires_at - datetime.utcnow()).total_seconds() * 1000))
            return {
                "key": key,
                "backend": "fallback",
                "redis_key": redis_key,
                "locked": handle is not None,
                "token": None if handle is None else handle.token,
                "ttl_ms": ttl_ms,
                "lease_ttl_seconds": self._lease_ttl_seconds,
                "heartbeat_interval_seconds": self._heartbeat_interval_seconds,
            }

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
            hasattr(self._client, attr) for attr in ("rpush", "lpop", "llen", "zadd", "zrangebyscore", "zrem", "delete", "exists", "scard", "sadd", "srem", "sismember", "zpopmin", "zcard")
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
        # Capability-aware in-process state
        self._capabilities: set[str] = set()
        self._cap_pending: defaultdict[str, list[tuple[int, str]]] = defaultdict(list)  # heap: (score, payload)
        self._cap_running: defaultdict[str, dict[str, float]] = defaultdict(dict)  # {cap: {task_id: ts}}
        self._cap_max_concurrent: dict[str, int] = {}  # optional per-cap limit

    # ------------------------------------------------------------------
    # Capability key helpers
    # ------------------------------------------------------------------

    def _cap_pending_key(self, capability: str) -> str:
        return f"{self._namespace}:{capability}:pending"

    def _cap_running_key(self, capability: str) -> str:
        return f"{self._namespace}:{capability}:running"

    def _cap_stats_key(self, capability: str) -> str:
        return f"{self._namespace}:{capability}:stats"

    def _cap_config_key(self, capability: str) -> str:
        return f"{self._namespace}:{capability}:config"

    def _cap_registry_key(self) -> str:
        return f"{self._namespace}:capabilities:registry"

    def _cap_task_data_key(self, capability: str) -> str:
        return f"{self._namespace}:{capability}:task_data"

    async def enqueue(self, task: Task, scheduled_at: datetime | None = None, capability: str = "default") -> None:  # type: ignore[override]
        serialized = self._serialize_task(task)
        cap = capability
        self._capabilities.add(cap)

        if self._client_supports_queue_ops:
            await self._client.srem(self._cancelled_set_key, task.id)
            # Register capability
            await self._client.sadd(self._cap_registry_key(), cap)

            if scheduled_at and scheduled_at > datetime.utcnow():
                # Store capability in delayed payload for later promotion
                delayed_payload = json.dumps({"capability": cap, "task": json.loads(serialized)})
                await self._client.zadd(self._delayed_key(), {delayed_payload: scheduled_at.timestamp()})
                await self._client.sadd(self._scheduled_set_key, task.id)
                return

            # Capability-aware: use ZSET with priority score
            ts_ms = int(time.time() * 1000)
            priority_rank = task.priority.value if hasattr(task.priority, 'value') else int(task.priority)
            score = priority_rank * (10 ** 13) + ts_ms
            pending_key = self._cap_pending_key(cap)
            task_data_key = self._cap_task_data_key(cap)
            stats_key = self._cap_stats_key(cap)

            await self._client.hset(task_data_key, task.id, serialized)
            await self._client.zadd(pending_key, {task.id: score})
            await self._client.hincrby(stats_key, "enqueue_count", 1)
            return

        # In-process fallback
        async with self._lock:
            if scheduled_at and scheduled_at > datetime.utcnow():
                await self._push_ready(serialized, task.priority.value, scheduled_at)
            else:
                ts_ms = int(time.time() * 1000)
                priority_rank = task.priority.value if hasattr(task.priority, 'value') else int(task.priority)
                score = priority_rank * (10 ** 13) + ts_ms
                import heapq
                heapq.heappush(self._cap_pending[cap], (score, serialized))
                self._task_index[task.id] = task

    async def dequeue(self, timeout: float | None = None, capability: str = "default") -> Task | None:  # type: ignore[override]
        await self._promote_due_tasks(capability=capability)
        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
        while True:
            if self._client_supports_queue_ops:
                pending_key = self._cap_pending_key(capability)
                running_key = self._cap_running_key(capability)
                task_data_key = self._cap_task_data_key(capability)
                stats_key = self._cap_stats_key(capability)
                config_key = self._cap_config_key(capability)

                # Check max_concurrent
                max_concurrent_raw = await self._client.hget(config_key, "max_concurrent")
                if max_concurrent_raw is not None:
                    max_concurrent = int(max_concurrent_raw)
                    running_count = await self._client.zcard(running_key)
                    if running_count >= max_concurrent:
                        return None

                # ZPOPMIN to get highest-priority (lowest-score) task
                result = await self._client.zpopmin(pending_key, 1)
                if not result:
                    # Fallback: also check legacy ready queues
                    for priority in sorted(TaskPriority, key=lambda p: p.value, reverse=False):
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
                    if timeout is None or asyncio.get_running_loop().time() >= deadline:
                        return None
                    await asyncio.sleep(0.01)
                    await self._promote_due_tasks(capability=capability)
                    continue

                task_id = result[0][0] if isinstance(result[0], (list, tuple)) else result[0]
                # Add to running ZSET
                ts_ms = int(time.time() * 1000)
                await self._client.zadd(running_key, {task_id: ts_ms})
                await self._client.hincrby(stats_key, "dequeue_count", 1)
                # Get task data
                payload = await self._client.hget(task_data_key, task_id)
                if not payload:
                    await self._client.zrem(running_key, task_id)
                    if timeout is None or asyncio.get_running_loop().time() >= deadline:
                        return None
                    await asyncio.sleep(0.01)
                    continue
                return self._deserialize_task(payload)
            else:
                # In-process fallback with capability awareness
                cap = capability
                max_concurrent = self._cap_max_concurrent.get(cap)
                if max_concurrent is not None and len(self._cap_running[cap]) >= max_concurrent:
                    return None

                import heapq
                heap = self._cap_pending[cap]
                while heap:
                    score, payload = heapq.heappop(heap)
                    task = self._deserialize_task(payload)
                    if task.id in self._cancelled_ids:
                        self._cancelled_ids.discard(task.id)
                        self._task_index.pop(task.id, None)
                        continue
                    self._queue_counts[task.priority.value] = max(0, self._queue_counts.get(task.priority.value, 0) - 1)
                    self._task_index.pop(task.id, None)
                    ts = time.time()
                    self._cap_running[cap][task.id] = ts
                    return task

                # Fallback to legacy ready_store if capability heap is empty
                for priority in sorted(TaskPriority, key=lambda p: p.value, reverse=False):
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
            await self._promote_due_tasks(capability=capability)

    async def peek(self, limit: int = 10) -> list[Task]:
        await self._promote_due_tasks()
        if self._client_supports_queue_ops:
            tasks: list[Task] = []
            for priority in sorted(TaskPriority, key=lambda p: p.value, reverse=False):
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
        for priority in sorted(TaskPriority, key=lambda p: p.value, reverse=False):
            values = self._ready_store[priority.value][:limit]
            for payload in values:
                task = self._deserialize_task(payload)
                if task.id in self._cancelled_ids:
                    continue
                tasks.append(task)
                if len(tasks) >= limit:
                    return tasks
        return tasks

    async def complete(self, task_id: str, capability: str = "default") -> None:
        """Mark a task as completed and remove from running set."""
        if self._client_supports_queue_ops:
            running_key = self._cap_running_key(capability)
            stats_key = self._cap_stats_key(capability)
            task_data_key = self._cap_task_data_key(capability)
            await self._client.zrem(running_key, task_id)
            await self._client.hincrby(stats_key, "complete_count", 1)
            await self._client.hdel(task_data_key, task_id)
        else:
            self._cap_running[capability].pop(task_id, None)

    async def fail(self, task_id: str, capability: str = "default") -> None:
        """Mark a task as failed and remove from running set."""
        if self._client_supports_queue_ops:
            running_key = self._cap_running_key(capability)
            stats_key = self._cap_stats_key(capability)
            task_data_key = self._cap_task_data_key(capability)
            await self._client.zrem(running_key, task_id)
            await self._client.hincrby(stats_key, "fail_count", 1)
            await self._client.hdel(task_data_key, task_id)
        else:
            self._cap_running[capability].pop(task_id, None)

    async def discover_capabilities(self) -> list[str]:
        """Return all known capability names."""
        if self._client_supports_queue_ops:
            caps = await self._client.smembers(self._cap_registry_key())
            return sorted(caps) if caps else []
        return sorted(self._capabilities)

    async def get_capability_stats(self, capability: str) -> dict:
        """Return stats for a capability queue."""
        if self._client_supports_queue_ops:
            stats_key = self._cap_stats_key(capability)
            running_key = self._cap_running_key(capability)
            pending_key = self._cap_pending_key(capability)
            raw = await self._client.hgetall(stats_key) or {}
            running_count = await self._client.zcard(running_key)
            pending_count = await self._client.zcard(pending_key)
            return {
                "capability": capability,
                "enqueue_count": int(raw.get("enqueue_count", 0)),
                "dequeue_count": int(raw.get("dequeue_count", 0)),
                "complete_count": int(raw.get("complete_count", 0)),
                "fail_count": int(raw.get("fail_count", 0)),
                "running_count": running_count,
                "pending_count": pending_count,
            }
        import heapq
        return {
            "capability": capability,
            "enqueue_count": 0,
            "dequeue_count": 0,
            "complete_count": 0,
            "fail_count": 0,
            "running_count": len(self._cap_running.get(capability, {})),
            "pending_count": len(self._cap_pending.get(capability, [])),
        }

    async def get_capability_pending(self, capability: str) -> int:
        """Return number of pending tasks for a capability."""
        if self._client_supports_queue_ops:
            return await self._client.zcard(self._cap_pending_key(capability))
        return len(self._cap_pending.get(capability, []))

    async def get_capability_running(self, capability: str) -> int:
        """Return number of running tasks for a capability."""
        if self._client_supports_queue_ops:
            return await self._client.zcard(self._cap_running_key(capability))
        return len(self._cap_running.get(capability, {}))

    async def set_max_concurrent(self, capability: str, max_concurrent: int) -> None:
        """Set max concurrent limit for a capability."""
        if self._client_supports_queue_ops:
            await self._client.hset(self._cap_config_key(capability), "max_concurrent", str(max_concurrent))
        else:
            self._cap_max_concurrent[capability] = max_concurrent

    async def cleanup_stale_running(self, capability: str = "default", max_age_seconds: float = 3600.0) -> int:
        """Remove stale entries from the running set."""
        if self._client_supports_queue_ops:
            running_key = self._cap_running_key(capability)
            stale_threshold = (time.time() - max_age_seconds) * 1000
            stale_ids = await self._client.zrangebyscore(running_key, "-inf", stale_threshold)
            if stale_ids:
                await self._client.zrem(running_key, *stale_ids)
            return len(stale_ids)
        else:
            now = time.time()
            running = self._cap_running.get(capability, {})
            stale = [tid for tid, ts in running.items() if now - ts > max_age_seconds]
            for tid in stale:
                running.pop(tid, None)
            return len(stale)

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
                # Remove from capability pending heaps
                removed_from_cap = False
                for cap, heap in self._cap_pending.items():
                    new_heap = [(s, p) for s, p in heap
                                if json.loads(p).get("id") != task_id]
                    if len(new_heap) < len(heap):
                        self._cap_pending[cap] = new_heap
                        import heapq
                        heapq.heapify(self._cap_pending[cap])
                        removed_from_cap = True
                if not removed_from_cap:
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
        # In-process: aggregate cap_pending by priority
        counts: dict[int, int] = {p.value: 0 for p in TaskPriority}
        for heap in self._cap_pending.values():
            for score, payload in heap:
                # Decode priority_rank from score
                rank = score // (10 ** 13)
                counts[rank] = counts.get(rank, 0) + 1
        return counts

    async def clear(self) -> None:
        if self._client_supports_queue_ops:
            keys = [self._ready_key(priority.value) for priority in TaskPriority]
            keys.append(self._delayed_key())
            keys.append(self._cancelled_set_key)
            keys.append(self._task_data_key)
            keys.append(self._scheduled_set_key)
            keys.append(self._cap_registry_key())
            # Clear all capability keys
            caps = await self.discover_capabilities()
            for cap in caps:
                keys.extend([
                    self._cap_pending_key(cap),
                    self._cap_running_key(cap),
                    self._cap_stats_key(cap),
                    self._cap_config_key(cap),
                    self._cap_task_data_key(cap),
                ])
            if keys:
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
            self._cap_pending.clear()
            self._cap_running.clear()
            self._capabilities.clear()

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
            caps = await self.discover_capabilities()
            # Use pipeline to batch all zcard + llen calls in a single RTT
            pipe = self._client.pipeline()
            for priority in TaskPriority:
                pipe.llen(self._ready_key(priority.value))
            for cap in caps:
                pipe.zcard(self._cap_pending_key(cap))
            results = await pipe.execute()
            return sum(results)
        # In-process: only count _cap_pending (canonical source)
        return sum(len(h) for h in self._cap_pending.values())

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

    async def _promote_due_tasks(self, capability: str = "default") -> None:
        now = datetime.utcnow().timestamp()
        if self._client_supports_queue_ops:
            # G6: Use Lua script for atomic time-gated promotion
            ts_ms = int(now * 1000)
            cap_filter = "*" if capability == "default" else capability
            try:
                promoted = await self._client.eval(
                    DELAYED_PROMOTE_CAP_SCRIPT,
                    3,
                    self._delayed_key(),
                    self._cap_registry_key(),
                    self._scheduled_set_key,
                    str(now),
                    str(ts_ms),
                    cap_filter,
                    self._namespace,
                )
                if promoted:
                    logger.debug("_promote_due_tasks: promoted %d tasks cap=%s", promoted, capability)
            except Exception as e:
                logger.warning("_promote_due_tasks Lua failed, fallback to Python: %s", e)
                # Fallback: manual Python promotion (non-atomic)
                due_payloads = await self._client.zrangebyscore(self._delayed_key(), float("-inf"), now)
                for payload in due_payloads:
                    try:
                        wrapped = json.loads(payload)
                        if isinstance(wrapped, dict) and "capability" in wrapped and "task" in wrapped:
                            cap = wrapped["capability"]
                            task_payload = json.dumps(wrapped["task"], sort_keys=True)
                            task = self._deserialize_task(task_payload)
                            if cap_filter != "*" and cap != cap_filter:
                                continue
                            priority_rank = task.priority.value if hasattr(task.priority, 'value') else int(task.priority)
                            score = priority_rank * (10 ** 13) + ts_ms
                            pending_key = self._cap_pending_key(cap)
                            task_data_key = self._cap_task_data_key(cap)
                            await self._client.sadd(self._cap_registry_key(), cap)
                            await self._client.hset(task_data_key, task.id, task_payload)
                            await self._client.zadd(pending_key, {task.id: score})
                            await self._client.zrem(self._delayed_key(), payload)
                            await self._client.srem(self._scheduled_set_key, task.id)
                    except (json.JSONDecodeError, KeyError):
                        pass
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
                    # Promote to capability pending heap
                    cap = capability
                    ts_ms = int(now * 1000)
                    priority_rank = task.priority.value if hasattr(task.priority, 'value') else int(task.priority)
                    score = priority_rank * (10 ** 13) + ts_ms
                    import heapq
                    heapq.heappush(self._cap_pending[cap], (score, payload))
                    self._capabilities.add(cap)

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
