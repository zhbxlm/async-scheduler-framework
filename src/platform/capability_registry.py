"""CapabilityRegistry — aligned with docs/deepwiki-reference/调度与资源管理.md

Three-state health FSM: healthy → degraded → unhealthy.
Debounce logic: needs consecutive N successes/failures to transition.
Storage: Redis JSON per capability, health counters in separate Hash.

Refactored to inherit BaseRedisRegistry for unified interface.
"""
from __future__ import annotations

import orjson
import logging
from typing import Any

from src.models.capability import CapabilityInfo, HealthStatus
from src.platform.base_registry import BaseRedisRegistry
from src.common.error_handling import log_errors, ExternalServiceError

logger = logging.getLogger(__name__)

_CAP_INDEX = "capabilities:all"
_HEALTH_KEY = "capability_health:{name}"   # hash: status, consec_ok, consec_fail

# Lua: atomic health FSM transition
_LUA_UPDATE_HEALTH = """
local hkey   = KEYS[1]
local capkey = KEYS[2]
local ok     = tonumber(ARGV[1])     -- 1=success 0=failure
local unhealthy_thresh = tonumber(ARGV[2])
local healthy_thresh   = tonumber(ARGV[3])

local status     = redis.call('HGET', hkey, 'status') or 'healthy'
local consec_ok  = tonumber(redis.call('HGET', hkey, 'consec_ok')  or '0')
local consec_fail= tonumber(redis.call('HGET', hkey, 'consec_fail') or '0')

if ok == 1 then
    consec_ok   = consec_ok + 1
    consec_fail = 0
else
    consec_fail = consec_fail + 1
    consec_ok   = 0
end

local new_status = status
if (status == 'unhealthy' or status == 'degraded') and consec_ok >= healthy_thresh then
    new_status = 'healthy'
elseif status == 'healthy' and consec_fail >= 2 then
    new_status = 'degraded'
elseif status == 'degraded' and consec_fail >= unhealthy_thresh then
    new_status = 'unhealthy'
end

redis.call('HSET', hkey, 'status', new_status,
           'consec_ok', tostring(consec_ok), 'consec_fail', tostring(consec_fail))

local raw = redis.call('GET', capkey)
if raw then
    local cap = cjson.decode(raw)
    cap['health_status'] = new_status
    redis.call('SET', capkey, cjson.encode(cap))
end

return new_status
"""

# Sentinel tenant for global (no-tenant) capabilities
_GLOBAL_TENANT = "_global_"


class CapabilityRegistry(BaseRedisRegistry):
    """Redis-backed capability registry with three-state health FSM.

    Inherits BaseRedisRegistry for unified CRUD interface.
    Capabilities are global (no tenant isolation), so tenant_id="_global_".
    """

    def __init__(self, redis_client: Any) -> None:
        super().__init__(
            redis_client=redis_client,
            key_prefix="capability",
            ttl_seconds=0,          # capabilities don't expire
        )
        self._lua_health = redis_client.register_script(_LUA_UPDATE_HEALTH)

    # ------------------------------------------------------------------
    # High-level API (domain methods used by routes)
    # ------------------------------------------------------------------

    @log_errors(log_level="ERROR", raise_exception=True, exception_type=ExternalServiceError)
    async def register(self, cap: CapabilityInfo) -> None:
        """Register or update a capability + initialise health counters."""
        await self.set(_GLOBAL_TENANT, cap.capability_name, cap.model_dump())
        await self._r.sadd(_CAP_INDEX, cap.capability_name)
        hkey = _HEALTH_KEY.format(name=cap.capability_name)
        await self._r.hset(hkey, mapping={
            "status": cap.health_status.value,
            "consec_ok": "0",
            "consec_fail": "0",
        })
        logger.info("CapabilityRegistry.register name=%s", cap.capability_name)

    @log_errors(log_level="WARNING", raise_exception=False)
    async def get_capability(self, name: str) -> CapabilityInfo | None:
        """Get a capability by name."""
        data = await self.get(_GLOBAL_TENANT, name)
        if data is None:
            return None
        return CapabilityInfo.model_validate(data)

    # Backward-compatible single-arg get (used by tests and existing callers)
    async def get(self, name_or_tenant: str, item_id: str | None = None) -> CapabilityInfo | None:  # type: ignore[override]
        """Get capability by name. Supports both get(name) and get(tenant, name) forms."""
        if item_id is None:
            # Legacy single-arg call: get(name)
            return await self.get_capability(name_or_tenant)
        # Two-arg call from BaseRedisRegistry (internal)
        data = await super().get(name_or_tenant, item_id)
        if data is None:
            return None
        return CapabilityInfo.model_validate(data)

    @log_errors(log_level="WARNING", raise_exception=False)
    async def find_capability(self, name: str) -> CapabilityInfo | None:
        """Return capability only if healthy or degraded (filters UNHEALTHY)."""
        cap = await self.get_capability(name)
        if cap is None:
            return None
        if cap.health_status == HealthStatus.UNHEALTHY:
            logger.warning("CapabilityRegistry: capability %s is UNHEALTHY, filtered", name)
            return None
        return cap

    @log_errors(log_level="ERROR", raise_exception=False)
    async def list_all(self) -> list[CapabilityInfo]:
        """Return all registered capabilities (pipelined for performance)."""
        names = await self._r.smembers(_CAP_INDEX)
        if not names:
            return []
        
        # Batch all GETs in a single pipeline
        pipe = self._r.pipeline()
        for raw_name in names:
            name = raw_name.decode() if isinstance(raw_name, bytes) else raw_name
            key = self._make_key(_GLOBAL_TENANT, name)
            pipe.get(key)
        results = await pipe.execute()
        
        caps: list[CapabilityInfo] = []
        for raw_name, data in zip(names, results, strict=False):
            if not data:
                continue
            name = raw_name.decode() if isinstance(raw_name, bytes) else raw_name
            try:
                parsed = orjson.loads(data)
                caps.append(CapabilityInfo.model_validate(parsed))
            except Exception as exc:
                logger.warning(
                    "CapabilityRegistry.list_all: skip %s: %s", name, exc
                )
        return caps

    @log_errors(log_level="ERROR", raise_exception=True, exception_type=ExternalServiceError)
    async def unregister(self, name: str) -> None:
        """Remove a capability from registry."""
        await self.delete(_GLOBAL_TENANT, name)
        await self._r.delete(_HEALTH_KEY.format(name=name))
        await self._r.srem(_CAP_INDEX, name)

    @log_errors(log_level="ERROR", raise_exception=True, exception_type=ExternalServiceError)
    async def update_health(
        self,
        name: str,
        success: bool,
        *,
        unhealthy_threshold: int = 3,
        healthy_threshold: int = 2,
    ) -> HealthStatus:
        """Update health FSM for *name* given a check result."""
        hkey = _HEALTH_KEY.format(name=name)
        capkey = self._make_key(_GLOBAL_TENANT, name)
        result = await self._lua_health(
            keys=[hkey, capkey],
            args=["1" if success else "0", str(unhealthy_threshold), str(healthy_threshold)],
        )
        status_str = result.decode() if isinstance(result, bytes) else str(result)
        status = HealthStatus(status_str)
        if status == HealthStatus.UNHEALTHY and not success:
            logger.warning("CapabilityRegistry: %s → UNHEALTHY", name)
        elif status == HealthStatus.HEALTHY and success:
            logger.info("CapabilityRegistry: %s recovered → HEALTHY", name)
        return status
