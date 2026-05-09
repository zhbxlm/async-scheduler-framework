"""task-api package-owned schedule registry."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from scheduler_task_api.infra.base_registry import BaseRedisRegistry

logger = logging.getLogger(__name__)
_SCHEDULE_TENANT_IDX = "schedules:sid_to_tenant"

_LUA_TOGGLE = """
local key = KEYS[1]
local idx_key = KEYS[2]
local schedule_id, enabled_str = ARGV[1], ARGV[2]
local raw = redis.call('GET', key)
if not raw then return 0 end
local data = cjson.decode(raw)
data['enabled'] = (enabled_str == '1')
redis.call('SET', key, cjson.encode(data))
return 1
"""

_LUA_ADVANCE_NEXT_FIRE = """
local key = KEYS[1]
local next_fire_iso = ARGV[1]
local raw = redis.call('GET', key)
if not raw then return 0 end
local data = cjson.decode(raw)
data['next_fire_at'] = next_fire_iso
data['last_triggered_at'] = ARGV[2]
redis.call('SET', key, cjson.encode(data))
return 1
"""


class ScheduleRegistry(BaseRedisRegistry):
    def __init__(self, redis_client: Any, ttl_seconds: int = 31_536_000):
        super().__init__(redis_client, "schedules", ttl_seconds)
        self._toggle_script = redis_client.register_script(_LUA_TOGGLE)
        self._advance_script = redis_client.register_script(_LUA_ADVANCE_NEXT_FIRE)

    async def set(self, tenant_id: str, schedule_id: str, value: Any) -> bool:
        result = await super().set(tenant_id, schedule_id, value)
        if result:
            await self._r.hset(_SCHEDULE_TENANT_IDX, schedule_id, tenant_id)
        return result

    async def toggle(self, tenant_id: str, schedule_id: str, enabled: bool) -> bool:
        key = self._make_key(tenant_id, schedule_id)
        idx_key = f"{self._prefix}:index:{tenant_id}"
        result = await self._toggle_script(keys=[key, idx_key], args=[schedule_id, "1" if enabled else "0"])
        return bool(result)

    async def advance_next_fire(self, tenant_id: str, schedule_id: str, next_fire: datetime) -> bool:
        key = self._make_key(tenant_id, schedule_id)
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        result = await self._advance_script(keys=[key], args=[next_fire.isoformat(), now_iso])
        return bool(result)

    async def list_due(self, as_of: datetime) -> list[Any]:
        due = []
        tenant_ids = await self.list_all_tenants()
        schedule_keys: list[tuple[str, str]] = []
        for tenant_id in tenant_ids:
            sids = await self.list(tenant_id)
            for sid in sids:
                schedule_keys.append((tenant_id, sid))
        if not schedule_keys:
            return due
        pipe = self._r.pipeline()
        for tid, sid in schedule_keys:
            pipe.get(self._make_key(tid, sid))
        raws = await pipe.execute()
        for (tid, sid), raw in zip(schedule_keys, raws):
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("ScheduleRegistry: bad JSON for %s:%s", tid, sid)
                continue
            if not data.get("enabled", True):
                continue
            nf = data.get("next_fire_at")
            if not nf:
                continue
            try:
                nf_dt = datetime.fromisoformat(nf)
                if nf_dt.tzinfo is None:
                    nf_dt = nf_dt.replace(tzinfo=timezone.utc)
                if nf_dt <= as_of:
                    due.append(_ScheduleProxy(data))
            except (ValueError, TypeError):
                logger.warning("ScheduleRegistry: bad next_fire_at=%s sid=%s", nf, sid)
        return due

    async def update_next_fire(self, schedule_id: str, next_fire: datetime) -> None:
        tenant_id = await self._r.hget(_SCHEDULE_TENANT_IDX, schedule_id)
        if tenant_id:
            if isinstance(tenant_id, bytes):
                tenant_id = tenant_id.decode()
            await self.advance_next_fire(tenant_id, schedule_id, next_fire)
            return
        tenant_ids = await self.list_all_tenants()
        for tid in tenant_ids:
            schedule_ids = await self.list(tid)
            if schedule_id in schedule_ids:
                await self.advance_next_fire(tid, schedule_id, next_fire)
                await self._r.hset(_SCHEDULE_TENANT_IDX, schedule_id, tid)
                return


class _ScheduleProxy:
    def __init__(self, data: dict):
        self._d = data

    def __getattr__(self, name: str) -> Any:
        try:
            return self._d[name]
        except KeyError:
            raise AttributeError(name) from None
