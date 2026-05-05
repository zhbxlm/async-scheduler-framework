"""ScheduleRegistry — aligned with docs/deepwiki-reference/Cron 调度.md

Cron schedule CRUD with:
- Atomic toggle (Lua CAS)
- Atomic next_fire_at advance
- list_due() for CronScheduler
"""
from __future__ import annotations
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from src.platform.base_registry import BaseRedisRegistry

logger = logging.getLogger(__name__)

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
    """Manages cron schedule definitions in Redis."""

    def __init__(self, redis_client: Any, ttl_seconds: int = 31_536_000):
        super().__init__(redis_client, "schedules", ttl_seconds)
        self._toggle_script = redis_client.register_script(_LUA_TOGGLE)
        self._advance_script = redis_client.register_script(_LUA_ADVANCE_NEXT_FIRE)

    async def toggle(self, tenant_id: str, schedule_id: str, enabled: bool) -> bool:
        """Atomically enable/disable a schedule."""
        key = self._make_key(tenant_id, schedule_id)
        idx_key = f"{self._prefix}:index:{tenant_id}"
        result = await self._toggle_script(
            keys=[key, idx_key],
            args=[schedule_id, "1" if enabled else "0"],
        )
        return bool(result)

    async def advance_next_fire(
        self,
        tenant_id: str,
        schedule_id: str,
        next_fire: datetime,
    ) -> bool:
        """Atomically advance next_fire_at after trigger."""
        key = self._make_key(tenant_id, schedule_id)
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        result = await self._advance_script(
            keys=[key],
            args=[next_fire.isoformat(), now_iso],
        )
        return bool(result)

    async def list_due(self, as_of: datetime) -> list[Any]:
        """Return all enabled schedules whose next_fire_at <= as_of.
        
        Optimised: pipeline batch-fetches all schedules in one Redis round-trip
        instead of O(tenants × schedules) individual GETs.
        """
        due = []
        tenant_ids = await self.list_all_tenants()

        # Phase 1: collect all schedule IDs across all tenants (no Redis calls)
        schedule_keys: list[tuple[str, str]] = []  # [(tenant, sid), ...]
        for tenant_id in tenant_ids:
            sids = await self.list(tenant_id)
            for sid in sids:
                schedule_keys.append((tenant_id, sid))

        if not schedule_keys:
            return due

        # Phase 2: batch GET all schedules in one pipeline
        pipe = self._r.pipeline()
        for tid, sid in schedule_keys:
            pipe.get(self._make_key(tid, sid))
        raws = await pipe.execute()

        # Phase 3: parse + filter locally (no more Redis calls)
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
        """Scan all tenants to find and update the schedule."""
        tenant_ids = await self.list_all_tenants()
        for tenant_id in tenant_ids:
            schedule_ids = await self.list(tenant_id)
            if schedule_id in schedule_ids:
                await self.advance_next_fire(tenant_id, schedule_id, next_fire)
                return


class _ScheduleProxy:
    """Lightweight proxy wrapping raw schedule dict for CronScheduler access."""

    def __init__(self, data: dict):
        self._d = data

    def __getattr__(self, name: str) -> Any:
        try:
            return self._d[name]
        except KeyError:
            raise AttributeError(name) from None
