"""QueueManager — aligned with docs/deepwiki-reference/队列管理.md

Priority queue with Lua atomic operations:
- Score encoding: priority_rank * 10^13 + timestamp_ms
- dequeue_ready: time-gated dequeue (execute_after_ms)
- Circuit-breaker integration via CircuitBreaker
- Step-level concurrency slots: acquire_concurrency_slot / release_slot
- Stale running cleanup
- Capabilities registry (SADD on enqueue, avoid SCAN)
"""
from __future__ import annotations

import logging
import time
from typing import Any

import redis.asyncio as aioredis

from src.platform import queue_keys as qk
from src.common.metrics import record_task_creation, record_task_completion

logger = logging.getLogger(__name__)

_DEFAULT_MAX_QUEUE_DEPTH = 1000
_DEFAULT_MAX_CONCURRENT = 8
_DEFAULT_TTL = 3600
_DEQUEUE_SCAN_LIMIT = 50
_CAPABILITIES_KEY = "queue:capabilities:registry"

# ---------------------------------------------------------------------------
# Lua scripts
# ---------------------------------------------------------------------------

_LUA_ENQUEUE = """
local pending_key, stats_key, config_key, running_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local task_id, score, default_depth, ttl = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local max_depth = tonumber(redis.call('HGET', config_key, 'max_queue_depth') or default_depth)
if not max_depth or max_depth <= 0 then max_depth = default_depth end
local cur = redis.call('ZCARD', pending_key)
if cur >= max_depth then
    return {-1, cur}
end
redis.call('ZADD', pending_key, score, task_id)
redis.call('HINCRBY', stats_key, 'total_enqueued', 1)
local cnt = redis.call('ZCARD', pending_key)
-- conditional TTL refresh
for _, k in ipairs({pending_key, stats_key, config_key}) do
    local remaining = redis.call('TTL', k)
    if remaining == -1 or remaining < ttl / 2 then
        redis.call('EXPIRE', k, ttl)
    end
end
return {cur + 1, cnt}
"""

_LUA_DEQUEUE_READY = """
local pending_key, running_key, stats_key, config_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local default_mc, now_ms, scan_limit = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local max_concurrent = tonumber(redis.call('HGET', config_key, 'max_concurrent') or default_mc)
if not max_concurrent or max_concurrent <= 0 then max_concurrent = default_mc end
local running_cnt = redis.call('ZCARD', running_key)
if running_cnt >= max_concurrent then return nil end
local candidates = redis.call('ZRANGEBYSCORE', pending_key, '-inf', '+inf', 'WITHSCORES', 'LIMIT', 0, scan_limit)
for i = 1, #candidates, 2 do
    local tid = candidates[i]
    local sc = tonumber(candidates[i+1])
    local ts = sc % 10000000000000
    if ts <= now_ms then
        redis.call('ZREM', pending_key, tid)
        redis.call('ZADD', running_key, now_ms, tid)
        redis.call('HINCRBY', stats_key, 'total_dequeued', 1)
        return tid
    end
end
return nil
"""

_LUA_CANCEL = """
local pending_key, running_key = KEYS[1], KEYS[2]
local task_id = ARGV[1]
local removed = redis.call('ZREM', pending_key, task_id)
if removed == 1 then return 1 end
return redis.call('ZREM', running_key, task_id)
"""

_LUA_COMPLETE = """
local running_key, stats_key = KEYS[1], KEYS[2]
local task_id, counter = ARGV[1], ARGV[2]
redis.call('ZREM', running_key, task_id)
redis.call('HINCRBY', stats_key, counter, 1)
return 1
"""

_LUA_ADJUST_CONCURRENT = """
local config_key = KEYS[1]
local delta, default_val = tonumber(ARGV[1]), tonumber(ARGV[2])
local cur = tonumber(redis.call('HGET', config_key, 'max_concurrent') or default_val)
if not cur then cur = default_val end
local new_val = math.max(1, cur + delta)
redis.call('HSET', config_key, 'max_concurrent', tostring(new_val))
return new_val
"""

_LUA_ACQUIRE_SLOT = """
local running_key, config_key = KEYS[1], KEYS[2]
local slot_id, default_mc, now_ts, ttl = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local max_concurrent = tonumber(redis.call('HGET', config_key, 'max_concurrent') or default_mc)
if not max_concurrent then max_concurrent = default_mc end
local cur = redis.call('ZCARD', running_key)
if cur >= max_concurrent then return -1 end
redis.call('ZADD', running_key, now_ts, slot_id)
local remaining = redis.call('TTL', running_key)
if remaining == -1 or remaining < ttl / 2 then
    redis.call('EXPIRE', running_key, ttl)
    redis.call('EXPIRE', config_key, ttl)
end
return 1
"""

_LUA_RECOVER_CONCURRENT = """
local config_key = KEYS[1]
local default_baseline = tonumber(ARGV[1])
local baseline = tonumber(redis.call('HGET', config_key, 'max_concurrent_baseline') or default_baseline)
if not baseline then baseline = default_baseline end
local cur = tonumber(redis.call('HGET', config_key, 'max_concurrent') or baseline)
if not cur then cur = baseline end
if cur < baseline then
    local new_val = cur + 1
    redis.call('HSET', config_key, 'max_concurrent', tostring(new_val))
    return new_val
end
return cur
"""


class QueueManager:
    """Priority-aware queue manager with circuit-breaker integration."""

    def __init__(
        self,
        redis_client: aioredis.Redis,
        *,
        circuit_breaker: Any | None = None,
        default_max_queue_depth: int = _DEFAULT_MAX_QUEUE_DEPTH,
        default_max_concurrent: int = _DEFAULT_MAX_CONCURRENT,
        ttl: int = _DEFAULT_TTL,
        dequeue_scan_limit: int = _DEQUEUE_SCAN_LIMIT,
    ) -> None:
        self._r = redis_client
        self._cb = circuit_breaker
        self._default_depth = default_max_queue_depth
        self._default_mc = default_max_concurrent
        self._ttl = ttl
        self._scan_limit = dequeue_scan_limit
        # pre-register Lua scripts
        self._enqueue_script = self._r.register_script(_LUA_ENQUEUE)
        self._dequeue_ready_script = self._r.register_script(_LUA_DEQUEUE_READY)
        self._cancel_script = self._r.register_script(_LUA_CANCEL)
        self._complete_script = self._r.register_script(_LUA_COMPLETE)
        self._adjust_script = self._r.register_script(_LUA_ADJUST_CONCURRENT)
        self._acquire_slot_script = self._r.register_script(_LUA_ACQUIRE_SLOT)
        self._recover_script = self._r.register_script(_LUA_RECOVER_CONCURRENT)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def enqueue(
        self,
        capability: str,
        task_id: str,
        priority: int = 3,          # 1–5; 1=highest
        execute_after_ms: int = 0,
    ) -> dict:
        """Enqueue task. Returns accepted/position/pending_count."""
        now_ms = int(time.time() * 1000)
        ts = execute_after_ms if execute_after_ms > now_ms else now_ms
        score = priority * 10_000_000_000_000 + ts

        keys = [qk.pending(capability), qk.stats(capability),
                qk.config(capability), qk.running(capability)]
        result = await self._enqueue_script(
            keys=keys,
            args=[task_id, score, self._default_depth, self._ttl],
        )
        if result is None or (isinstance(result, list) and result[0] == -1):
            pending_cnt = result[1] if isinstance(result, list) else -1
            return {"accepted": False, "queue_position": -1, "pending_count": pending_cnt}

        pos, cnt = result
        # register capability
        await self._r.sadd(_CAPABILITIES_KEY, capability)
        
        # Record task creation metric
        priority_map_reverse = {
            1: "very_high",
            2: "high",
            3: "normal",
            4: "low",
            5: "tide"
        }
        priority_str = priority_map_reverse.get(priority, "normal")
        record_task_creation(capability, priority_str)
        
        return {"accepted": True, "queue_position": int(pos), "pending_count": int(cnt)}

    async def dequeue_ready(self, capability: str) -> str | None:
        """Dequeue next ready task (execute_after_ms <= now)."""
        now_ms = int(time.time() * 1000)
        keys = [qk.pending(capability), qk.running(capability),
                qk.stats(capability), qk.config(capability)]
        result = await self._dequeue_ready_script(
            keys=keys,
            args=[self._default_mc, now_ms, self._scan_limit],
        )
        if result is None:
            return None
        return result.decode() if isinstance(result, bytes) else result

    async def cancel(self, capability: str, task_id: str) -> bool:
        """Cancel a pending or running task."""
        keys = [qk.pending(capability), qk.running(capability)]
        removed = await self._cancel_script(keys=keys, args=[task_id])
        return bool(removed)

    async def complete(self, capability: str, task_id: str) -> bool:
        """Mark task completed — releases running slot."""
        keys = [qk.running(capability), qk.stats(capability)]
        await self._complete_script(keys=keys, args=[task_id, "total_completed"])
        if self._cb:
            await self._cb.record_success(capability)
        return True

    async def fail(self, capability: str, task_id: str) -> bool:
        """Mark task failed — releases running slot."""
        keys = [qk.running(capability), qk.stats(capability)]
        await self._complete_script(keys=keys, args=[task_id, "total_failed"])
        if self._cb:
            await self._cb.record_failure(capability)
        return True

    async def release_running_slot(self, capability: str, task_id: str) -> bool:
        """Release running slot without updating completed/failed counters."""
        await self._r.zrem(qk.running(capability), task_id)
        return True

    # ------------------------------------------------------------------
    # Back-pressure / circuit-breaker
    # ------------------------------------------------------------------

    async def adjust_concurrent(self, capability: str, delta: int, *, slot: bool = False) -> int:
        """Adjust max_concurrent by delta (min=1). slot=True → step-level config."""
        cfg_key = qk.slot_config(capability) if slot else qk.config(capability)
        new_val = await self._adjust_script(keys=[cfg_key], args=[delta, self._default_mc])
        return int(new_val)

    async def try_recover_concurrent(self, capability: str) -> int:
        """Gradually restore max_concurrent toward baseline (call on success path)."""
        cfg_key = qk.config(capability)
        new_val = await self._recover_script(keys=[cfg_key], args=[self._default_mc])
        return int(new_val)

    # ------------------------------------------------------------------
    # Step-level concurrency slots
    # ------------------------------------------------------------------

    async def acquire_concurrency_slot(
        self,
        capability: str,
        slot_id: str,
        max_concurrent: int | None = None,
    ) -> bool:
        """Acquire a step-level concurrency slot. Returns False if full."""
        now_ts = int(time.time() * 1000)
        mc = max_concurrent or self._default_mc
        keys = [qk.slot_running(capability), qk.slot_config(capability)]
        result = await self._acquire_slot_script(
            keys=keys,
            args=[slot_id, mc, now_ts, self._ttl],
        )
        return int(result) == 1

    async def release_slot(self, capability: str, slot_id: str) -> bool:
        """Release a step-level concurrency slot."""
        await self._r.zrem(qk.slot_running(capability), slot_id)
        return True

    # ------------------------------------------------------------------
    # Stale cleanup
    # ------------------------------------------------------------------

    async def cleanup_stale_running(
        self, capability: str, max_age_seconds: float = 300.0
    ) -> int:
        """Remove stale entries from task-level running set."""
        cutoff_ms = int((time.time() - max_age_seconds) * 1000)
        stale = await self._r.zrangebyscore(qk.running(capability), "-inf", cutoff_ms)
        if not stale:
            return 0
        pipe = self._r.pipeline()
        for tid in stale:
            pipe.zrem(qk.running(capability), tid)
        await pipe.execute()
        logger.info("cleanup_stale_running: capability=%s removed=%d", capability, len(stale))
        return len(stale)

    async def cleanup_stale_slots(
        self, capability: str, max_age_seconds: float = 300.0
    ) -> int:
        """Remove stale entries from slot-level running set."""
        cutoff_ms = int((time.time() - max_age_seconds) * 1000)
        stale = await self._r.zrangebyscore(qk.slot_running(capability), "-inf", cutoff_ms)
        if not stale:
            return 0
        pipe = self._r.pipeline()
        for sid in stale:
            pipe.zrem(qk.slot_running(capability), sid)
        await pipe.execute()
        return len(stale)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    async def get_queue_snapshot(self, capability: str) -> dict:
        """Return queue depth + config for scaling decisions."""
        pipe = self._r.pipeline()
        pipe.zcard(qk.pending(capability))
        pipe.zcard(qk.running(capability))
        pipe.hget(qk.config(capability), "max_queue_depth")
        pipe.hget(qk.config(capability), "max_concurrent")
        results = await pipe.execute()
        pending_cnt, running_cnt, max_depth_raw, max_mc_raw = results
        return {
            "capability": capability,
            "pending": int(pending_cnt or 0),
            "running": int(running_cnt or 0),
            "max_queue_depth": int(max_depth_raw or self._default_depth),
            "max_concurrent": int(max_mc_raw or self._default_mc),
        }

    async def discover_queue_capabilities(self) -> list[str]:
        """Return all registered capability names (registry-first, SCAN fallback)."""
        members = await self._r.smembers(_CAPABILITIES_KEY)
        if members:
            return [m.decode() if isinstance(m, bytes) else m for m in members]
        # Fallback: SCAN for queue:*:pending keys
        caps = set()
        async for key in self._r.scan_iter("*:pending"):
            key_str = key.decode() if isinstance(key, bytes) else key
            # Extract capability from key format {queue:CAP}:pending
            if ":pending" in key_str:
                cap = key_str.split("}")[0].lstrip("{queue:").lstrip("queue:").strip("{")
                if cap:
                    caps.add(cap)
        if caps:
            await self._r.sadd(_CAPABILITIES_KEY, *caps)
        return list(caps)

    # dequeue alias for task_consumer compatibility
    async def dequeue(self, capability: str) -> dict | None:
        """Alias: dequeue_ready → return dict or None."""
        task_id = await self.dequeue_ready(capability)
        if task_id is None:
            return None
        return {"task_id": task_id, "capability": capability}
