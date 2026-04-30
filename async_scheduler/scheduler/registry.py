"""Schedule registry abstraction.

This is a local/SQLite-backed counterpart of the deepwiki ScheduleRegistry,
introduced to keep lifecycle logic separate from CronScheduler.
"""

from __future__ import annotations

from datetime import datetime

from croniter import croniter

from async_scheduler.core.models import Schedule, ScheduleCreate
from async_scheduler.persistence import ScheduleRepository, get_session_no_context


class ScheduleRegistry:
    """Registry facade for schedule lifecycle operations."""

    async def create(self, schedule_create: ScheduleCreate) -> Schedule:
        async with await get_session_no_context() as session:
            created = await ScheduleRepository.create(session, schedule_create)
            return created

    async def get(self, schedule_id: str) -> Schedule | None:
        async with await get_session_no_context() as session:
            return await ScheduleRepository.get(session, schedule_id)

    async def list_active(self, limit: int = 100) -> list[Schedule]:
        async with await get_session_no_context() as session:
            return await ScheduleRepository.list_active(session, limit=limit)

    async def list_ready(self, now: datetime | None = None) -> list[Schedule]:
        now = now or datetime.utcnow()
        schedules = await self.list_active(limit=1000)
        ready: list[Schedule] = []
        for schedule in schedules:
            if schedule.next_run_at is None:
                next_fire = croniter(schedule.cron_expression, now).get_next(datetime)
                await self.advance_next_fire(schedule.id, next_fire, last_triggered_at=schedule.last_run_at)
                continue
            if schedule.next_run_at <= now:
                ready.append(schedule)
        return ready

    async def advance_next_fire(
        self,
        schedule_id: str,
        next_fire_at: datetime,
        *,
        last_triggered_at: datetime | None,
    ) -> Schedule | None:
        async with await get_session_no_context() as session:
            return await ScheduleRepository.update(
                session,
                schedule_id,
                next_run_at=next_fire_at,
                last_run_at=last_triggered_at,
            )

    async def pause(self, schedule_id: str) -> Schedule | None:
        async with await get_session_no_context() as session:
            from async_scheduler.core.models import ScheduleStatus

            return await ScheduleRepository.update(session, schedule_id, status=ScheduleStatus.PAUSED)

    async def resume(self, schedule_id: str) -> Schedule | None:
        async with await get_session_no_context() as session:
            from async_scheduler.core.models import ScheduleStatus

            schedule = await ScheduleRepository.get(session, schedule_id)
            if schedule is None:
                return None
            next_fire = croniter(schedule.cron_expression, datetime.utcnow()).get_next(datetime)
            return await ScheduleRepository.update(
                session,
                schedule_id,
                status=ScheduleStatus.ACTIVE,
                next_run_at=next_fire,
            )
