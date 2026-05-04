"""QuotaEnforcer — aligned with docs/deepwiki-reference/配额与多租户.md

Atomic check-and-increment / decrement operations via Redis Lua scripts.
Tracks three dimensions: task_count, gpu_count, actor_count.
Zero quota value = unlimited.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from src.models.tenant import TenantQuota

logger = logging.getLogger(__name__)

# Redis key: usage hash per tenant
_USAGE_KEY = "tenant_usage:{tenant_id}"
# Redis key: tenant info JSON
_TENANT_KEY = "tenant:{tenant_id}"
# Redis key: all tenant IDs set
_TENANT_INDEX = "tenants:all"

FIELD_TASK_COUNT = "task_count"
FIELD_GPU_COUNT = "gpu_count"
FIELD_ACTOR_COUNT = "actor_count"


class QuotaExceededError(Exception):
    """Raised when a quota limit would be exceeded."""
    def __init__(self, tenant_id: str, resource: str, limit: int, current: int) -> None:
        self.tenant_id = tenant_id
        self.resource = resource
        self.limit = limit
        self.current = current
        super().__init__(
            f"Quota exceeded for tenant={tenant_id!r}: "
            f"{resource}={current} >= limit={limit}"
        )


# ---------------------------------------------------------------------------
# Lua scripts (atomic check + increment / decrement + clamp)
# ---------------------------------------------------------------------------

_LUA_CHECK_AND_INCREMENT = """
local key   = KEYS[1]
local field = ARGV[1]
local delta = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])   -- 0 means unlimited

local current = tonumber(redis.call('HGET', key, field) or '0')
if limit > 0 and current + delta > limit then
    return {err='quota_exceeded:' .. tostring(current) .. ':' .. tostring(limit)}
end
redis.call('HINCRBY', key, field, delta)
return tostring(current + delta)
"""

_LUA_DECREMENT_CLAMP = """
local key   = KEYS[1]
local field = ARGV[1]
local delta = tonumber(ARGV[2])

local current = tonumber(redis.call('HGET', key, field) or '0')
local new_val = math.max(0, current - delta)
redis.call('HSET', key, field, tostring(new_val))
return tostring(new_val)
"""


class QuotaEnforcer:
    """Atomic quota enforcement via Redis Lua scripts.

    Parameters
    ----------
    redis_client:
        Async Redis client.
    default_quota:
        Default quota applied when tenant has no explicit quota.
    """

    def __init__(
        self,
        redis_client: Any,
        default_quota: TenantQuota | None = None,
    ) -> None:
        self._r = redis_client
        self._default_quota = default_quota or TenantQuota()

    # ------------------------------------------------------------------
    # Task quota
    # ------------------------------------------------------------------

    async def reserve_task_submission(self, tenant_id: str) -> None:
        """Check and reserve one task slot.  Raises QuotaExceededError."""
        quota = await self._get_quota(tenant_id)
        await self._increment(tenant_id, FIELD_TASK_COUNT, 1, quota.max_queue_depth)

    async def decrement_task_count(self, tenant_id: str, delta: int = 1) -> None:
        """Release *delta* task slots (clamped to 0)."""
        await self._decrement(tenant_id, FIELD_TASK_COUNT, delta)

    # ------------------------------------------------------------------
    # GPU quota
    # ------------------------------------------------------------------

    async def reserve_gpu_allocation(self, tenant_id: str, gpus: int) -> None:
        """Check and reserve *gpus* GPU slots.  Raises QuotaExceededError."""
        if gpus <= 0:
            return
        quota = await self._get_quota(tenant_id)
        await self._increment(tenant_id, FIELD_GPU_COUNT, gpus, quota.max_gpus)

    async def decrement_gpu_count(self, tenant_id: str, gpus: int = 1) -> None:
        await self._decrement(tenant_id, FIELD_GPU_COUNT, gpus)

    # ------------------------------------------------------------------
    # Actor quota
    # ------------------------------------------------------------------

    async def reserve_actor_creation(self, tenant_id: str, count: int = 1) -> None:
        quota = await self._get_quota(tenant_id)
        await self._increment(tenant_id, FIELD_ACTOR_COUNT, count, quota.max_actor_count)

    async def decrement_actor_count(self, tenant_id: str, count: int = 1) -> None:
        await self._decrement(tenant_id, FIELD_ACTOR_COUNT, count)

    # ------------------------------------------------------------------
    # Usage inspection
    # ------------------------------------------------------------------

    async def get_usage(self, tenant_id: str) -> dict[str, int]:
        """Return current usage counters for *tenant_id*."""
        key = _USAGE_KEY.format(tenant_id=tenant_id)
        raw = await self._r.hgetall(key)
        result: dict[str, int] = {
            FIELD_TASK_COUNT: 0,
            FIELD_GPU_COUNT: 0,
            FIELD_ACTOR_COUNT: 0,
        }
        for k, v in raw.items():
            field = k.decode() if isinstance(k, bytes) else k
            result[field] = int(v)
        return result

    async def reconcile_quota_counters(
        self,
        tenant_id: str,
        *,
        actual_task_count: int | None = None,
        actual_gpu_count: int | None = None,
        actual_actor_count: int | None = None,
    ) -> dict[str, int]:
        """Correct counter drift by setting fields to known-good values.

        Skips any field passed as None.  Returns dict of changes.
        """
        key = _USAGE_KEY.format(tenant_id=tenant_id)
        changes: dict[str, int] = {}
        for field, actual in [
            (FIELD_TASK_COUNT, actual_task_count),
            (FIELD_GPU_COUNT, actual_gpu_count),
            (FIELD_ACTOR_COUNT, actual_actor_count),
        ]:
            if actual is None:
                continue
            current_raw = await self._r.hget(key, field)
            current = int(current_raw) if current_raw else 0
            drift = current - max(0, actual)
            if drift != 0:
                await self._r.hset(key, field, str(max(0, actual)))
                changes[field] = drift
                logger.warning(
                    "QuotaEnforcer: reconcile tenant=%s field=%s drift=%+d",
                    tenant_id, field, drift,
                )
        return changes

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _increment(
        self, tenant_id: str, field: str, delta: int, limit: int
    ) -> None:
        key = _USAGE_KEY.format(tenant_id=tenant_id)
        result = await self._r.eval(
            _LUA_CHECK_AND_INCREMENT, 1, key, field, str(delta), str(limit)
        )
        if isinstance(result, Exception):
            raise result
        result_str = result.decode() if isinstance(result, bytes) else str(result)
        if result_str.startswith("quota_exceeded"):
            parts = result_str.split(":")
            current = int(parts[1]) if len(parts) > 1 else 0
            lim = int(parts[2]) if len(parts) > 2 else limit
            raise QuotaExceededError(tenant_id, field, lim, current)

    async def _decrement(self, tenant_id: str, field: str, delta: int) -> None:
        key = _USAGE_KEY.format(tenant_id=tenant_id)
        await self._r.eval(_LUA_DECREMENT_CLAMP, 1, key, field, str(delta))

    async def _get_quota(self, tenant_id: str) -> TenantQuota:
        key = _TENANT_KEY.format(tenant_id=tenant_id)
        raw = await self._r.get(key)
        if raw is None:
            return self._default_quota
        try:
            data = json.loads(raw)
            quota_data = data.get("quota", {})
            return TenantQuota.model_validate(quota_data)
        except Exception:
            return self._default_quota
