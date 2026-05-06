"""CronScheduler — aligned with docs/deepwiki-reference/Cron 调度.md

Periodic loop with Redis distributed leader election.
Idempotent task creation via idempotency_key to prevent duplicate fires.
"""
from __future__ import annotations

import asyncio
import orjson
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from croniter import croniter
from src.common.error_handling import log_errors

logger = logging.getLogger(__name__)

_LEADER_KEY = "cron_leader"
_LEADER_TTL = 90  # seconds


class CronScheduler:
    """Cron schedule runner with leader election."""

    def __init__(
        self,
        redis_client: Any,
        schedule_repository: Any,
        task_creator: Any,
        *,
        instance_id: str | None = None,
        poll_interval: float = 60.0,
        leader_ttl: int = _LEADER_TTL,
    ) -> None:
        self._r = redis_client
        self._schedules = schedule_repository
        self._creator = task_creator
        self._instance_id = instance_id or str(uuid.uuid4())
        self._poll_interval = poll_interval
        self._leader_ttl = leader_ttl
        self._running = False

    async def start(self) -> None:
        self._running = True
        logger.info("CronScheduler started id=%s", self._instance_id)
        await self._run_loop()

    async def stop(self) -> None:
        self._running = False

    @log_errors(log_level="WARNING", raise_exception=False)
    async def _run_loop(self) -> None:
        while self._running:
            if await self._try_become_leader():
                await self._fire_due_schedules()
            await asyncio.sleep(self._poll_interval)

    async def _try_become_leader(self) -> bool:
        """Redis SET NX EX for leader election."""
        ok = await self._r.set(_LEADER_KEY, self._instance_id, nx=True, ex=self._leader_ttl)
        if ok:
            return True
        current = await self._r.get(_LEADER_KEY)
        current_id = current.decode() if isinstance(current, bytes) else current
        if current_id == self._instance_id:
            await self._r.expire(_LEADER_KEY, self._leader_ttl)
            return True
        return False

    async def _fire_due_schedules(self) -> None:
        now = datetime.now(tz=timezone.utc)
        due = await self._schedules.list_due(now)
        for schedule in due:
            await self._fire_one(schedule, now)

    async def _fire_one(self, schedule: Any, now: datetime) -> None:
        schedule_id = getattr(schedule, "schedule_id", str(schedule))
        cron_expr = getattr(schedule, "cron_expr", "")
        tenant_id = getattr(schedule, "tenant_id", "")
        task_template_raw = getattr(schedule, "task_template", "{}")

        try:
            template = orjson.loads(task_template_raw) if isinstance(task_template_raw, str) else task_template_raw
        except Exception:
            template = {}

        # Idempotency key: schedule_id + cron window timestamp
        idem_key = f"cron:{schedule_id}:{int(now.timestamp() // 60 * 60)}"

        try:
            await self._creator.create_task(
                tenant_id=tenant_id,
                idempotency_key=idem_key,
                **template,
            )
        except Exception as e:
            logger.warning("CronScheduler: fire %s failed: %s", schedule_id, e)
            return

        # Advance next_fire_at
        cron_iter = croniter(cron_expr, now)
        next_fire = cron_iter.get_next(datetime)
        await self._schedules.update_next_fire(schedule_id, next_fire)
        logger.info("CronScheduler: fired schedule=%s next=%s", schedule_id, next_fire.isoformat())
