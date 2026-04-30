"""Cron scheduler for recurring task schedules."""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Awaitable, Callable

from croniter import croniter

from async_scheduler.core.models import Schedule, ScheduleStatus, TaskCreate, TaskPriority
from async_scheduler.persistence import get_session_no_context, ScheduleRepository, TaskRepository
from async_scheduler.queue import QueueManager
from async_scheduler.scheduler.registry import ScheduleRegistry

logger = logging.getLogger(__name__)


class CronScheduler:
    """Schedules recurring tasks based on cron expressions."""

    def __init__(
        self,
        queue_manager: QueueManager,
        poll_interval: float = 60.0,
    ) -> None:
        """Initialize the cron scheduler."""
        self._queue_manager = queue_manager
        self._poll_interval = poll_interval
        self._running = False
        self._scheduler_task: asyncio.Task[None] | None = None
        self._last_check: datetime | None = None
        self._registry = ScheduleRegistry()

    async def start(self) -> None:
        """Start the cron scheduler."""
        if self._running:
            logger.warning("Cron scheduler is already running")
            return

        self._running = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info("Cron scheduler started")

    async def stop(self) -> None:
        """Stop the cron scheduler."""
        if not self._running:
            return

        self._running = False

        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        logger.info("Cron scheduler stopped")

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            try:
                await self._process_schedules()
                self._last_check = datetime.utcnow()
                await asyncio.sleep(self._poll_interval)
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                await asyncio.sleep(self._poll_interval)

    async def _process_schedules(self) -> None:
        """Process all active schedules and create tasks if due."""
        schedules = await self._registry.list_ready(datetime.utcnow())
        async with await get_session_no_context() as session:
            for schedule in schedules:
                try:
                    await self._process_single_schedule(session, schedule)
                except Exception as e:
                    logger.error(f"Error processing schedule {schedule.id}: {e}", exc_info=True)

    async def _process_single_schedule(
        self,
        session,
        schedule: Schedule,
    ) -> None:
        """Process a single schedule."""
        now = datetime.utcnow()

        if schedule.next_run_at is None or schedule.next_run_at > now:
            return

        # Check if we haven't run recently (prevent duplicate runs)
        if schedule.last_run_at:
            time_since_last_run = (now - schedule.last_run_at).total_seconds()
            if time_since_last_run < schedule.dedup_window_seconds:
                return

        # Create task from schedule
        task = await self._create_task_from_schedule(schedule)

        # Enqueue task
        await self._queue_manager.enqueue(task)

        # Calculate next run time
        cron = croniter(schedule.cron_expression, now)
        next_run = cron.get_next(datetime)

        # Update schedule
        await self._registry.advance_next_fire(
            schedule.id,
            next_run,
            last_triggered_at=now,
        )

        logger.info(f"Created task {task.id} from schedule {schedule.id}")

    async def _create_task_from_schedule(self, schedule: Schedule) -> Awaitable:
        """Create a task from a schedule template."""
        async with await get_session_no_context() as session:
            now = datetime.utcnow()
            bucket = int(now.timestamp() // max(schedule.dedup_window_seconds, 1))
            task_create = TaskCreate(
                name=schedule.name,
                payload=schedule.task_template,
                priority=TaskPriority.NORMAL,
                tenant_id=schedule.tenant_id,
                scheduled_at=now,
                idempotency_key=f"schedule:{schedule.id}:{bucket}",
            )

            return await TaskRepository.create(session, task_create)

    async def trigger_schedule(self, schedule_id: str) -> bool:
        """Manually trigger a schedule execution."""
        async with await get_session_no_context() as session:
            schedule = await ScheduleRepository.get(session, schedule_id)

            if not schedule or schedule.status != ScheduleStatus.ACTIVE:
                return False

            # Create task immediately
            task = await self._create_task_from_schedule(schedule)

            # Enqueue task
            await self._queue_manager.enqueue(task)

            # Update last run time
            await ScheduleRepository.update(
                session,
                schedule.id,
                last_run_at=datetime.utcnow(),
            )

            logger.info(f"Manually triggered schedule {schedule_id}, created task {task.id}")
            return True

    def is_running(self) -> bool:
        """Check if the scheduler is running."""
        return self._running

    def get_last_check(self) -> datetime | None:
        """Get the timestamp of the last schedule check."""
        return self._last_check
