"""Tenant quota enforcement with Redis-backed atomic check-and-increment.

Aligned with deepwiki ray-async QuotaEnforcer spec:
- Per-tenant usage counters stored in Redis Hash ``tenant_usage:{tenant_id}``
- Lua scripts guarantee atomic check+increment (avoids TOCTOU races)
- Three quota dimensions: task_count, gpu_count, actor_count
- Zero quota value means unlimited
- Decrement-with-clamp on release (prevents negative counters)

Falls back to in-process counters when no Redis client is available
(development / test environments).
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from textwrap import dedent
from typing import Any

logger = logging.getLogger(__name__)


class QuotaExceededError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Lua scripts
# ---------------------------------------------------------------------------

# Atomic check-and-increment.
# KEYS[1] = usage_hash_key
# ARGV[1] = field name  ARGV[2] = delta  ARGV[3] = max_value (0 = unlimited)
# Returns new value on success, -1 on quota exceeded.
_LUA_CHECK_INCREMENT = dedent(
    """
    local key   = KEYS[1]
    local field = ARGV[1]
    local delta = tonumber(ARGV[2])
    local limit = tonumber(ARGV[3])

    local current = tonumber(redis.call('HGET', key, field) or '0')
    local new_val = current + delta

    if limit > 0 and new_val > limit then
        return -1
    end
    redis.call('HSET', key, field, new_val)
    return new_val
    """
).strip()

# Decrement with clamp to zero.
# KEYS[1] = usage_hash_key
# ARGV[1] = field name  ARGV[2] = delta (positive; subtracted)
_LUA_DECREMENT_CLAMP = dedent(
    """
    local key   = KEYS[1]
    local field = ARGV[1]
    local delta = tonumber(ARGV[2])

    local current = tonumber(redis.call('HGET', key, field) or '0')
    local new_val = math.max(0, current - delta)
    redis.call('HSET', key, field, new_val)
    return new_val
    """
).strip()


class TenantQuotaManager:
    """Atomic per-tenant quota enforcement.

    Parameters
    ----------
    redis_client:
        Optional async Redis client.  When provided, usage counters are stored
        in Redis so quota limits are enforced across all workers.
    default_max_queued:
        Default queue-depth limit per tenant (maps to task_count quota).
    default_max_running:
        Default concurrency limit per tenant (maps to running_count quota).
    namespace:
        Key prefix for Redis hashes.
    """

    # Quota field names (align with deepwiki spec)
    FIELD_TASK_COUNT = "task_count"
    FIELD_RUNNING_COUNT = "running_count"
    FIELD_GPU_COUNT = "gpu_count"
    FIELD_ACTOR_COUNT = "actor_count"

    def __init__(
        self,
        redis_client: Any = None,
        *,
        default_max_queued: int = 100,
        default_max_running: int = 10,
        namespace: str = "async-scheduler",
    ) -> None:
        self._redis = redis_client
        self.default_max_queued = default_max_queued
        self.default_max_running = default_max_running
        self._namespace = namespace

        # In-process fallback state
        self._queued: dict[str, int] = defaultdict(int)
        self._running: dict[str, int] = defaultdict(int)
        self._gpu: dict[str, int] = defaultdict(int)
        self._actor: dict[str, int] = defaultdict(int)
        self._config: dict[str, dict[str, int]] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def configure_tenant(
        self,
        tenant_id: str,
        *,
        max_queued: int | None = None,
        max_running: int | None = None,
        max_gpu: int = 0,
        max_actor: int = 0,
    ) -> None:
        """Set quota limits for *tenant_id*.  0 means unlimited."""
        self._config[tenant_id] = {
            "max_queued": max_queued or self.default_max_queued,
            "max_running": max_running or self.default_max_running,
            "max_gpu": max_gpu,
            "max_actor": max_actor,
        }

    def _limits(self, tenant_id: str | None) -> dict[str, int]:
        key = tenant_id or "default"
        return self._config.get(
            key,
            {
                "max_queued": self.default_max_queued,
                "max_running": self.default_max_running,
                "max_gpu": 0,
                "max_actor": 0,
            },
        )

    # ------------------------------------------------------------------
    # Public quota operations
    # ------------------------------------------------------------------

    async def admit_queue(self, tenant_id: str | None) -> None:
        """Check and increment task_count; raise QuotaExceededError if over limit."""
        key = tenant_id or "default"
        limits = self._limits(key)
        await self._increment(key, self.FIELD_TASK_COUNT, 1, limits["max_queued"])

    async def release_queue(self, tenant_id: str | None) -> None:
        """Decrement task_count (clamped to 0)."""
        key = tenant_id or "default"
        await self._decrement(key, self.FIELD_TASK_COUNT, 1)

    async def admit_running(self, tenant_id: str | None) -> None:
        """Check and increment running_count; raise QuotaExceededError if over limit."""
        key = tenant_id or "default"
        limits = self._limits(key)
        await self._increment(key, self.FIELD_RUNNING_COUNT, 1, limits["max_running"])

    async def release_running(self, tenant_id: str | None) -> None:
        """Decrement running_count (clamped to 0)."""
        key = tenant_id or "default"
        await self._decrement(key, self.FIELD_RUNNING_COUNT, 1)

    async def admit_gpu(self, tenant_id: str | None, count: int = 1) -> None:
        """Check and increment gpu_count."""
        key = tenant_id or "default"
        limits = self._limits(key)
        await self._increment(key, self.FIELD_GPU_COUNT, count, limits.get("max_gpu", 0))

    async def release_gpu(self, tenant_id: str | None, count: int = 1) -> None:
        key = tenant_id or "default"
        await self._decrement(key, self.FIELD_GPU_COUNT, count)

    async def admit_actor(self, tenant_id: str | None, count: int = 1) -> None:
        """Check and increment actor_count."""
        key = tenant_id or "default"
        limits = self._limits(key)
        await self._increment(key, self.FIELD_ACTOR_COUNT, count, limits.get("max_actor", 0))

    async def release_actor(self, tenant_id: str | None, count: int = 1) -> None:
        key = tenant_id or "default"
        await self._decrement(key, self.FIELD_ACTOR_COUNT, count)

    async def get_usage(self, tenant_id: str | None) -> dict[str, int]:
        """Return current usage counters for *tenant_id*."""
        key = tenant_id or "default"
        if self._redis is not None:
            hash_key = self._usage_key(key)
            raw = await self._redis.hgetall(hash_key)
            return {k: int(v) for k, v in raw.items()}
        async with self._lock:
            return {
                self.FIELD_TASK_COUNT: self._queued[key],
                self.FIELD_RUNNING_COUNT: self._running[key],
                self.FIELD_GPU_COUNT: self._gpu[key],
                self.FIELD_ACTOR_COUNT: self._actor[key],
            }

    async def stats(self) -> dict[str, dict[str, Any]]:
        """Return per-tenant usage + limits (all known tenants).

        Response shape (per tenant)::

            {
                "task_count": <int>,
                "running_count": <int>,
                "gpu_count": <int>,
                "actor_count": <int>,
                "max_queued": <int>,
                "max_running": <int>,
                "max_gpu": <int>,
                "max_actor": <int>,
            }
        """
        all_keys = set(self._config) | {"default"}
        result: dict[str, dict[str, Any]] = {}
        for k in all_keys:
            usage = await self.get_usage(k)
            limits = self._limits(k)  # keys: max_queued, max_running, max_gpu, max_actor
            result[k] = {**usage, **limits}
        return result

    async def check_and_reserve(self, tenant_id: str | None, resource_type: str) -> None:
        """Doc-compatible API: check quota and reserve a slot.

        ``resource_type`` is one of ``"queued"`` or ``"running"``.
        Delegates to :meth:`admit_queue` / :meth:`admit_running`.
        Raises :class:`QuotaExceededError` when over limit.
        """
        if resource_type == "queued":
            await self.admit_queue(tenant_id)
        elif resource_type == "running":
            await self.admit_running(tenant_id)
        else:
            raise ValueError(f"Unknown resource_type: {resource_type!r}")

    async def release(self, tenant_id: str | None, resource_type: str) -> None:
        """Doc-compatible API: release a previously reserved quota slot.

        ``resource_type`` is one of ``"queued"`` or ``"running"``.
        Delegates to :meth:`release_queue` / :meth:`release_running`.
        """
        if resource_type == "queued":
            await self.release_queue(tenant_id)
        elif resource_type == "running":
            await self.release_running(tenant_id)
        else:
            raise ValueError(f"Unknown resource_type: {resource_type!r}")

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _usage_key(self, tenant_id: str) -> str:
        return f"{self._namespace}:tenant_usage:{tenant_id}"

    async def _increment(self, tenant_id: str, field: str, delta: int, max_val: int) -> int:
        if self._redis is not None:
            result = int(
                await self._redis.eval(
                    _LUA_CHECK_INCREMENT,
                    1,
                    self._usage_key(tenant_id),
                    field,
                    str(delta),
                    str(max_val),
                )
            )
            if result == -1:
                raise QuotaExceededError(
                    f"tenant {tenant_id!r} quota exceeded for field={field} limit={max_val}"
                )
            return result

        # In-process fallback
        async with self._lock:
            store = self._field_store(field)
            current = store[tenant_id]
            new_val = current + delta
            if max_val > 0 and new_val > max_val:
                raise QuotaExceededError(
                    f"tenant {tenant_id!r} quota exceeded for field={field} limit={max_val}"
                )
            store[tenant_id] = new_val
            return new_val

    async def _decrement(self, tenant_id: str, field: str, delta: int) -> int:
        if self._redis is not None:
            return int(
                await self._redis.eval(
                    _LUA_DECREMENT_CLAMP,
                    1,
                    self._usage_key(tenant_id),
                    field,
                    str(delta),
                )
            )
        async with self._lock:
            store = self._field_store(field)
            store[tenant_id] = max(0, store[tenant_id] - delta)
            return store[tenant_id]

    def _field_store(self, field: str) -> dict[str, int]:
        mapping = {
            self.FIELD_TASK_COUNT: self._queued,
            self.FIELD_RUNNING_COUNT: self._running,
            self.FIELD_GPU_COUNT: self._gpu,
            self.FIELD_ACTOR_COUNT: self._actor,
        }
        return mapping.get(field, self._queued)
