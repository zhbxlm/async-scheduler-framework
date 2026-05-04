# rebuilt from deepwiki-reference alignment
"""CircuitBreaker — 3-state (CLOSED/OPEN/HALF_OPEN) failure protection.

Implements the circuit breaker pattern for queue capability protection:
  CLOSED → (failures >= threshold) → OPEN
  OPEN   → (cooldown expired)      → HALF_OPEN
  HALF_OPEN → (success)            → CLOSED
  HALF_OPEN → (failure)            → OPEN
"""
from __future__ import annotations
import time
from typing import Any
import redis.asyncio as aioredis

_STATE_CLOSED    = "closed"
_STATE_OPEN      = "open"
_STATE_HALF_OPEN = "half_open"

# Lua: atomic record_failure — transitions CLOSED→OPEN when threshold exceeded
_LUA_RECORD_FAILURE = """
local state_key    = KEYS[1]
local count_key    = KEYS[2]
local threshold    = tonumber(ARGV[1])
local open_ttl     = tonumber(ARGV[2])
local now          = tonumber(ARGV[3])

local state = redis.call('GET', state_key) or 'closed'
if state == 'open' then return state end

local count = tonumber(redis.call('INCR', count_key))
redis.call('EXPIRE', count_key, open_ttl * 4)

if count >= threshold or state == 'half_open' then
    redis.call('SET',    state_key, 'open')
    redis.call('EXPIRE', state_key, open_ttl)
    redis.call('DEL',    count_key)
    return 'open'
end
return 'closed'
"""

# Lua: atomic record_success — HALF_OPEN→CLOSED, else reset counter
_LUA_RECORD_SUCCESS = """
local state_key = KEYS[1]
local count_key = KEYS[2]

local state = redis.call('GET', state_key) or 'closed'
if state == 'half_open' then
    redis.call('SET', state_key, 'closed')
    redis.call('DEL', count_key)
    return 'closed'
end
-- reset failure count on success in closed state
redis.call('DEL', count_key)
return state
"""

# Lua: check state — if OPEN and TTL expired, transition to HALF_OPEN
_LUA_CHECK_STATE = """
local state_key    = KEYS[1]
local ho_key       = KEYS[2]
local ho_max       = tonumber(ARGV[1])

local state = redis.call('GET', state_key)
if not state then return 'closed' end
if state == 'open' then
    -- if the key exists it still has TTL, still open
    return 'open'
end
if state == 'half_open' then
    local attempts = tonumber(redis.call('GET', ho_key) or '0')
    if attempts >= ho_max then return 'open' end
    redis.call('INCR', ho_key)
    redis.call('EXPIRE', ho_key, 120)
    return 'half_open'
end
return 'closed'
"""


class CircuitBreaker:
    """Redis-backed 3-state circuit breaker for capability queues."""

    def __init__(
        self,
        redis_client: aioredis.Redis,
        key_prefix: str,
        failure_threshold: int = 10,
        open_duration_seconds: int = 60,
        half_open_max: int = 1,
    ) -> None:
        self._r = redis_client
        self._prefix = key_prefix.rstrip(":")
        self._threshold = failure_threshold
        self._open_duration = open_duration_seconds
        self._half_open_max = half_open_max

    # ── key helpers ────────────────────────────────────────────────────────
    def _state_key(self, capability: str) -> str:
        return f"{self._prefix}:{capability}:cb:state"

    def _count_key(self, capability: str) -> str:
        return f"{self._prefix}:{capability}:cb:failures"

    def _half_open_key(self, capability: str) -> str:
        return f"{self._prefix}:{capability}:cb:half_open"

    # ── public API ─────────────────────────────────────────────────────────
    async def record_failure(self, capability: str) -> str:
        """Record a failure; returns new state ('closed'/'open')."""
        result = await self._r.eval(
            _LUA_RECORD_FAILURE, 2,
            self._state_key(capability), self._count_key(capability),
            str(self._threshold), str(self._open_duration), str(time.time()),
        )
        return result.decode() if isinstance(result, bytes) else str(result)

    async def record_success(self, capability: str) -> str:
        """Record a success; returns new state ('closed'/'half_open')."""
        result = await self._r.eval(
            _LUA_RECORD_SUCCESS, 2,
            self._state_key(capability), self._count_key(capability),
        )
        return result.decode() if isinstance(result, bytes) else str(result)

    async def get_state(self, capability: str) -> str:
        """Return current state, transitioning OPEN→HALF_OPEN if TTL expired."""
        result = await self._r.eval(
            _LUA_CHECK_STATE, 2,
            self._state_key(capability), self._half_open_key(capability),
            str(self._half_open_max),
        )
        return result.decode() if isinstance(result, bytes) else str(result)

    async def is_open(self, capability: str) -> bool:
        """Return True if circuit is OPEN (requests should be rejected)."""
        state = await self.get_state(capability)
        return state == _STATE_OPEN

    async def allow_request(self, capability: str) -> bool:
        """Return True if the circuit allows a request through."""
        state = await self.get_state(capability)
        return state in (_STATE_CLOSED, _STATE_HALF_OPEN)

    def get_stats(self) -> dict[str, Any]:
        """Return sync stats snapshot (for /callbacks/stats endpoint)."""
        return {
            "failure_threshold": self._threshold,
            "open_duration_seconds": self._open_duration,
            "half_open_max": self._half_open_max,
        }
