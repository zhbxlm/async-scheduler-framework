"""CapabilityRegistry — aligned with docs/deepwiki-reference/调度与资源管理.md

Three-state health FSM: healthy → degraded → unhealthy.
Debounce logic: needs consecutive N successes/failures to transition.
Storage: Redis JSON per capability, health counters in separate Hash.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from src.models.capability import CapabilityInfo, HealthStatus

logger = logging.getLogger(__name__)

_CAP_KEY = "capability:{name}"
_HEALTH_KEY = "capability_health:{name}"      # hash: status, consec_ok, consec_fail
_CAP_INDEX = "capabilities:all"

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

redis.call('HSET', hkey, 'status', new_status, 'consec_ok', tostring(consec_ok), 'consec_fail', tostring(consec_fail))

-- sync status back into capability JSON
local raw = redis.call('GET', capkey)
if raw then
    local cap = cjson.decode(raw)
    cap['health_status'] = new_status
    redis.call('SET', capkey, cjson.encode(cap))
end

return new_status
"""


class CapabilityRegistry:
    """Redis-backed capability registry with three-state health FSM."""

    def __init__(self, redis_client: Any) -> None:
        self._r = redis_client

    async def register(self, cap: CapabilityInfo) -> None:
        key = _CAP_KEY.format(name=cap.capability_name)
        await self._r.set(key, cap.model_dump_json())
        await self._r.sadd(_CAP_INDEX, cap.capability_name)
        hkey = _HEALTH_KEY.format(name=cap.capability_name)
        await self._r.hset(hkey, mapping={
            "status": cap.health_status.value,
            "consec_ok": "0",
            "consec_fail": "0",
        })
        logger.info("CapabilityRegistry.register name=%s", cap.capability_name)

    async def get(self, name: str) -> CapabilityInfo | None:
        key = _CAP_KEY.format(name=name)
        raw = await self._r.get(key)
        if raw is None:
            return None
        return CapabilityInfo.model_validate(json.loads(raw))

    async def find_capability(self, name: str) -> CapabilityInfo | None:
        """Return capability only if healthy or degraded (not unhealthy)."""
        cap = await self.get(name)
        if cap is None:
            return None
        if cap.health_status == HealthStatus.UNHEALTHY:
            logger.warning("CapabilityRegistry: capability %s is UNHEALTHY, filtered", name)
            return None
        return cap

    async def list_all(self) -> list[CapabilityInfo]:
        names = await self._r.smembers(_CAP_INDEX)
        caps = []
        for n in names:
            name = n.decode() if isinstance(n, bytes) else n
            cap = await self.get(name)
            if cap:
                caps.append(cap)
        return caps

    async def unregister(self, name: str) -> None:
        await self._r.delete(_CAP_KEY.format(name=name))
        await self._r.delete(_HEALTH_KEY.format(name=name))
        await self._r.srem(_CAP_INDEX, name)

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
        capkey = _CAP_KEY.format(name=name)
        result = await self._r.eval(
            _LUA_UPDATE_HEALTH,
            2, hkey, capkey,
            "1" if success else "0",
            str(unhealthy_threshold),
            str(healthy_threshold),
        )
        status_str = result.decode() if isinstance(result, bytes) else str(result)
        status = HealthStatus(status_str)
        if status == HealthStatus.UNHEALTHY and success is False:
            logger.warning("CapabilityRegistry: %s → UNHEALTHY", name)
        elif status == HealthStatus.HEALTHY and success:
            logger.info("CapabilityRegistry: %s recovered → HEALTHY", name)
        return status
