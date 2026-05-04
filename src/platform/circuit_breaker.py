"""CircuitBreaker — 3-state (CLOSED/OPEN/HALF_OPEN) failure protection."""
from __future__ import annotations
import time
from typing import Any
import redis.asyncio as aioredis


class CircuitBreaker:
    def __init__(
        self,
        redis_client: aioredis.Redis,
        key_prefix: str,
        failure_threshold: int = 10,
        open_duration_seconds: int = 60,
        half_open_max: int = 1,
    ):
        self._r = redis_client
        self._prefix = key_prefix.rstrip(":")
        self._failure_threshold = failure_threshold
        self._open_duration = open_duration_seconds
        self._half_open_max = half_open_max

    def _state_key(self) -> str:
        return f"{self._prefix}:state"

    def _count_key(self) -> str:
        return f"{self._prefix}:failure_count"

    def _half_open_key(self) -> str:
        return f"{self._prefix}:half_open_attempts"

    async def allow_request(self) -> bool:
        state = await self._r.get(self._state_key())
        if state is None:
            return True  # CLOSED
        state = state.decode() if isinstance(state, bytes) else state
        if state == "OPEN":
            # Check if open duration expired
            ttl = await self._r.ttl(self._state_key())
            if ttl <= 0:
                await self._transition_to_half_open()
                return True
            return False
        # HALF_OPEN
        attempts = await self._r.incr(self._half_open_key())
        if attempts > self._half_open_max:
            await self._transition_to_open()
            return False
        return True

    async def record_success(self) -> None:
        await self._reset()

    async def record_failure(self) -> None:
        count = await self._r.incr(self._count_key())
        if count >= self._failure_threshold:
            await self._transition_to_open()
        # Reset half-open attempts if any
        await self._r.delete(self._half_open_key())

    async def _reset(self) -> None:
        await self._r.delete(self._state_key())
        await self._r.delete(self._count_key())
        await self._r.delete(self._half_open_key())

    async def _transition_to_open(self) -> None:
        await self._r.setex(self._state_key(), self._open_duration, "OPEN")
        await self._r.delete(self._half_open_key())

    async def _transition_to_half_open(self) -> None:
        await self._r.set(self._state_key(), "HALF_OPEN")
        await self._r.set(self._half_open_key(), 0)
