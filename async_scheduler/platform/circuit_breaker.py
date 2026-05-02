"""Three-state circuit breaker backed by Redis.

Implements the CLOSED → OPEN → HALF_OPEN → CLOSED state machine described in
the deepwiki queue-management spec.  All state mutations happen inside Lua
scripts so that concurrent callers cannot corrupt the counters.

State transitions
-----------------
CLOSED → OPEN        : consecutive_failures reaches failure_threshold
OPEN   → HALF_OPEN   : open_duration_seconds has elapsed
HALF_OPEN → CLOSED   : half_open_successes reaches half_open_max
                       (also resets max_concurrent to baseline)
HALF_OPEN → HALF_OPEN: any failure resets half_open_successes counter
"""

from __future__ import annotations

import asyncio
import logging
from enum import Enum
from textwrap import dedent
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lua scripts
# ---------------------------------------------------------------------------

# Check current circuit state; auto-advance OPEN → HALF_OPEN when cooldown passes.
# KEYS[1] = stats_hash_key
# ARGV[1] = now_seconds (float string)
# ARGV[2] = open_duration_seconds
# ARGV[3] = failure_threshold
# Returns {blocked (0|1), transition_str}
_LUA_CIRCUIT_CHECK = dedent(
    """
    local stats  = KEYS[1]
    local now    = tonumber(ARGV[1])
    local dur    = tonumber(ARGV[2])
    local thresh = tonumber(ARGV[3])

    local state   = redis.call('HGET', stats, 'circuit_state') or 'closed'
    local blocked = 0
    local trans   = ''

    if state == 'open' then
        local opened_at = tonumber(redis.call('HGET', stats, 'circuit_opened_at') or '0')
        if now - opened_at >= dur then
            redis.call('HSET', stats, 'circuit_state', 'half_open')
            redis.call('HSET', stats, 'half_open_successes', 0)
            trans = 'open->half_open'
        else
            blocked = 1
        end
    elseif state == 'half_open' then
        blocked = 0
    else
        -- closed: check if threshold reached
        local fails = tonumber(redis.call('HGET', stats, 'consecutive_failures') or '0')
        if fails >= thresh then
            redis.call('HSET', stats, 'circuit_state', 'open')
            redis.call('HSET', stats, 'circuit_opened_at', now)
            blocked = 1
            trans = 'closed->open'
        end
    end
    return {blocked, trans}
    """
).strip()

# Record a call result; drive state machine.
# KEYS[1] = stats_hash_key  KEYS[2] = config_hash_key
# ARGV[1] = success (1|0)   ARGV[2] = half_open_max
# Returns transition_str (empty if none)
_LUA_RECORD_RESULT = dedent(
    """
    local stats       = KEYS[1]
    local config      = KEYS[2]
    local success     = tonumber(ARGV[1])
    local ho_max      = tonumber(ARGV[2])
    local trans       = ''

    local state = redis.call('HGET', stats, 'circuit_state') or 'closed'

    if success == 1 then
        redis.call('HSET', stats, 'consecutive_failures', 0)
        if state == 'half_open' then
            local ho_ok = tonumber(redis.call('HINCRBY', stats, 'half_open_successes', 1))
            if ho_ok >= ho_max then
                redis.call('HSET', stats, 'circuit_state', 'closed')
                redis.call('HSET', stats, 'consecutive_failures', 0)
                trans = 'half_open->closed'
                -- restore max_concurrent to baseline
                local baseline = tonumber(redis.call('HGET', config, 'max_concurrent_baseline'))
                if baseline then
                    redis.call('HSET', config, 'max_concurrent', baseline)
                end
            end
        end
    else
        redis.call('HINCRBY', stats, 'consecutive_failures', 1)
        if state == 'half_open' then
            redis.call('HSET', stats, 'half_open_successes', 0)
        end
    end
    return trans
    """
).strip()

# Gradually restore max_concurrent toward baseline (+1 per call, capped at baseline).
# KEYS[1] = config_hash_key   ARGV[1] = default_baseline
# Returns new max_concurrent value
_LUA_RECOVER_CONCURRENT = dedent(
    """
    local config   = KEYS[1]
    local baseline = tonumber(redis.call('HGET', config, 'max_concurrent_baseline'))
                     or tonumber(ARGV[1])
    local current  = tonumber(redis.call('HGET', config, 'max_concurrent'))
                     or baseline
    if current < baseline then
        local nv = current + 1
        redis.call('HSET', config, 'max_concurrent', nv)
        return nv
    end
    return current
    """
).strip()

# Atomically adjust max_concurrent by delta (min 1).
# KEYS[1] = config_hash_key   ARGV[1] = delta   ARGV[2] = default_value
_LUA_ADJUST_CONCURRENT = dedent(
    """
    local config  = KEYS[1]
    local delta   = tonumber(ARGV[1])
    local default = tonumber(ARGV[2])
    local current = tonumber(redis.call('HGET', config, 'max_concurrent')) or default
    local nv      = math.max(1, current + delta)
    redis.call('HSET', config, 'max_concurrent', nv)
    return nv
    """
).strip()


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Redis-backed three-state circuit breaker.

    Parameters
    ----------
    stats_key:
        Redis hash key holding state counters (shared with QueueManager stats).
    config_key:
        Redis hash key holding queue config (for max_concurrent baseline).
    failure_threshold:
        Consecutive failures before tripping to OPEN.
    open_duration_seconds:
        How long to stay OPEN before advancing to HALF_OPEN.
    half_open_max:
        Successful probes needed to return to CLOSED.
    client:
        Async Redis client (redis.asyncio compatible).
    """

    def __init__(
        self,
        stats_key: str,
        config_key: str,
        *,
        failure_threshold: int = 10,
        open_duration_seconds: float = 60.0,
        half_open_max: int = 1,
        client: Any = None,
    ) -> None:
        self._stats_key = stats_key
        self._config_key = config_key
        self.failure_threshold = failure_threshold
        self.open_duration_seconds = open_duration_seconds
        self.half_open_max = half_open_max
        self._client = client

        # In-memory fallback for environments without Redis
        self._state: CircuitState = CircuitState.CLOSED
        self._consecutive_failures: int = 0
        self._half_open_successes: int = 0
        self._opened_at: float | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def is_open(self) -> bool:
        """Return True if the circuit is OPEN (requests should be rejected)."""
        blocked, transition = await self._check()
        if transition:
            logger.info("circuit_breaker state transition: %s key=%s", transition, self._stats_key)
        return bool(blocked)

    async def record_success(self) -> None:
        """Record a successful call; may close the circuit."""
        trans = await self._record(success=True)
        if trans:
            logger.info("circuit_breaker state transition: %s key=%s", trans, self._stats_key)

    async def record_failure(self) -> None:
        """Record a failed call; may open the circuit."""
        trans = await self._record(success=False)
        if trans:
            logger.info("circuit_breaker state transition: %s key=%s", trans, self._stats_key)

    async def try_recover_concurrent(self, default_baseline: int = 8) -> int:
        """Incrementally restore max_concurrent toward baseline.  Returns new value."""
        if self._client is not None:
            return int(
                await self._client.eval(_LUA_RECOVER_CONCURRENT, 1, self._config_key, str(default_baseline))
            )
        return default_baseline

    async def adjust_concurrent(self, delta: int, default_value: int = 8) -> int:
        """Atomically adjust max_concurrent by *delta* (minimum 1). Returns new value."""
        if self._client is not None:
            return int(
                await self._client.eval(
                    _LUA_ADJUST_CONCURRENT, 1, self._config_key, str(delta), str(default_value)
                )
            )
        return max(1, default_value + delta)

    async def get_state(self) -> CircuitState:
        """Return current circuit state without side effects."""
        if self._client is not None:
            raw = await self._client.hget(self._stats_key, "circuit_state")
            return CircuitState(raw) if raw else CircuitState.CLOSED
        # In-process: apply time-based transition passively so get_state() is consistent
        import time as _t
        async with self._lock:
            if self._state == CircuitState.OPEN:
                if self._opened_at is not None and _t.time() - self._opened_at >= self.open_duration_seconds:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_successes = 0
            return self._state

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _check(self) -> tuple[int, str]:
        import time
        now = time.time()
        if self._client is not None:
            result = await self._client.eval(
                _LUA_CIRCUIT_CHECK,
                1,
                self._stats_key,
                str(now),
                str(self.open_duration_seconds),
                str(self.failure_threshold),
            )
            return int(result[0]), str(result[1]) if result[1] else ""

        # In-memory fallback
        async with self._lock:
            import time as _t
            if self._state == CircuitState.OPEN:
                if self._opened_at is not None and _t.time() - self._opened_at >= self.open_duration_seconds:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_successes = 0
                    return 0, "open->half_open"
                return 1, ""
            if self._state == CircuitState.HALF_OPEN:
                return 0, ""
            # CLOSED: check if failures reached threshold
            if self._consecutive_failures >= self.failure_threshold:
                self._state = CircuitState.OPEN
                self._opened_at = _t.time()
                return 1, "closed->open"
            return 0, ""

    async def _record(self, *, success: bool) -> str:
        if self._client is not None:
            result = await self._client.eval(
                _LUA_RECORD_RESULT,
                2,
                self._stats_key,
                self._config_key,
                "1" if success else "0",
                str(self.half_open_max),
            )
            return str(result) if result else ""

        # In-memory fallback
        async with self._lock:
            if success:
                self._consecutive_failures = 0
                if self._state == CircuitState.HALF_OPEN:
                    self._half_open_successes += 1
                    if self._half_open_successes >= self.half_open_max:
                        self._state = CircuitState.CLOSED
                        self._half_open_successes = 0
                        return "half_open->closed"
            else:
                if self._state == CircuitState.HALF_OPEN:
                    # In HALF_OPEN, failure resets probe counter but stays HALF_OPEN
                    self._half_open_successes = 0
                else:
                    # CLOSED: increment failure counter (OPEN stays OPEN)
                    if self._state == CircuitState.CLOSED:
                        self._consecutive_failures += 1
            return ""
