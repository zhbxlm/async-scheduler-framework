"""Cron scheduler for recurring task schedules with distributed leader election."""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable

from croniter import croniter

from async_scheduler.core.models import Schedule, ScheduleStatus, TaskCreate, TaskPriority
from async_scheduler.persistence import get_session_no_context, ScheduleRepository, TaskRepository
from async_scheduler.queue import QueueManager
from async_scheduler.scheduler.registry import ScheduleRegistry

logger = logging.getLogger(__name__)


class CronScheduler:
    """Schedules recurring tasks based on cron expressions.

    Distributed Leader Election
    ---------------------------
    When a Redis client is provided (``redis_client`` parameter), the scheduler
    participates in distributed leader election using a Redis SET NX lease.
    Only the instance holding the lease executes schedule processing;
    other instances wait and retry on each poll interval.  This prevents
    duplicate task creation when multiple replicas run CronScheduler.

    Idempotency
    -----------
    Each scheduled task uses an ``idempotency_key`` of the form
    ``schedule:{id}:{bucket}`` where ``bucket = floor(now / dedup_window)``.
    This prevents duplicate creation within the same trigger window even if
    the lease is briefly held by two nodes during failover.
    """

    _LEADER_LEASE_KEY = "cron:scheduler:leader"
    _LEADER_LEASE_TTL = 90  # seconds – must be > poll_interval

    def __init__(
        self,
        queue_manager: QueueManager,
        poll_interval: float = 60.0,
        redis_client: Any | None = None,
        leader_lease_ttl: int = 90,
        instance_id: str | None = None,
    ) -> None:
        """Initialize the cron scheduler.

        Parameters
        ----------
        queue_manager:
            Queue manager to enqueue scheduled tasks.
        poll_interval:
            Seconds between schedule checks.
        redis_client:
            Optional async Redis client for distributed leader election.
        leader_lease_ttl:
            TTL in seconds for the leader lease key.
        instance_id:
            Unique identifier for this scheduler instance (default: random UUID).
        """
        import uuid as _uuid
        self._queue_manager = queue_manager
        self._poll_interval = poll_interval
        self._redis = redis_client
        self._leader_lease_ttl = leader_lease_ttl
        self._instance_id = instance_id or str(_uuid.uuid4())
        self._leader_key = self._LEADER_LEASE_KEY
        self._running = False
        self._scheduler_task: asyncio.Task[None] | None = None
        self._last_check: datetime | None = None
        self._is_leader: bool = False
        self._registry = ScheduleRegistry()
        self._leader_renew_task: asyncio.Task[None] | None = None
        self._metrics: dict[str, Any] = {
            "poll_iterations": 0,
            "leader_acquired_count": 0,
            "leader_lost_count": 0,
            "schedule_process_errors": 0,
            "schedule_processed_count": 0,
            "last_error": None,
        }

    async def start(self) -> None:
        """Start the cron scheduler."""
        if self._running:
            logger.warning("Cron scheduler is already running")
            return

        self._running = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        self._leader_renew_task = asyncio.create_task(self._leader_renewal_loop())
        logger.info(
            "Cron scheduler started instance_id=%s poll_interval=%s leader_ttl=%s redis=%s",
            self._instance_id,
            self._poll_interval,
            self._leader_lease_ttl,
            self._redis is not None,
        )

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

        # P1-TODO-7: stop leader renewal task
        if hasattr(self, "_leader_renew_task") and self._leader_renew_task:
            self._leader_renew_task.cancel()
            try:
                await self._leader_renew_task
            except asyncio.CancelledError:
                pass

        logger.info("Cron scheduler stopped")

    async def _try_acquire_leader_lease(self) -> bool:
        """Try to acquire or renew the distributed leader lease.

        Returns True if this instance is the leader.
        """
        if self._redis is None:
            return True  # single-node mode: always leader

        key = self._leader_key
        token = self._instance_id
        ttl = self._leader_lease_ttl

        # Try SET NX (acquire if not held)
        acquired = await self._redis.set(key, token, ex=ttl, nx=True)
        if acquired:
            self._is_leader = True
            self._metrics["leader_acquired_count"] += 1
            logger.info("CronScheduler leadership acquired instance_id=%s", self._instance_id)
            return True

        # Check if we already own it (renew)
        current = await self._redis.get(key)
        if current == token:
            await self._redis.expire(key, ttl)
            self._is_leader = True
            return True

        self._is_leader = False
        return False

    async def _renew_leader_lease(self) -> bool:
        """Renew leader lease. Returns False if we lost leadership."""
        if self._redis is None:
            return True  # in-process mode, always leader
        key = self._leader_key
        token = self._instance_id
        ttl = self._leader_lease_ttl
        # Only set if key exists AND value matches (xx=True)
        result = await self._redis.set(key, token, ex=ttl, xx=True)
        if result is None:
            self._is_leader = False
            self._metrics["leader_lost_count"] += 1
            return False
        self._is_leader = True
        return True

    async def _leader_renewal_loop(self) -> None:
        """Background loop to renew leader lease periodically."""
        # Renew at 1/3 of TTL to stay well ahead of expiry
        renew_interval = self._leader_lease_ttl / 3
        while self._running:
            await asyncio.sleep(renew_interval)
            if self._is_leader:
                renewed = await self._renew_leader_lease()
                if not renewed:
                    logger.warning(
                        "CronScheduler lost leadership, will re-elect on next poll instance_id=%s",
                        self._instance_id,
                    )

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop with distributed leader election."""
        while self._running:
            try:
                self._metrics["poll_iterations"] += 1
                is_leader = await self._try_acquire_leader_lease()
                if is_leader:
                    await self._process_schedules()
                    self._last_check = datetime.utcnow()
                else:
                    logger.debug(
                        "cron scheduler: not leader instance_id=%s; skipping",
                        self._instance_id,
                    )
                await asyncio.sleep(self._poll_interval)
            except Exception as e:
                self._metrics["last_error"] = str(e)
                logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                await asyncio.sleep(self._poll_interval)

    async def _process_schedules(self) -> None:
        """Process all active schedules and create tasks if due."""
        schedules = await self._registry.list_ready(datetime.utcnow())
        async with await get_session_no_context() as session:
            for schedule in schedules:
                try:
                    await self._process_single_schedule(session, schedule)
                    self._metrics["schedule_processed_count"] += 1
                except Exception as e:
                    self._metrics["schedule_process_errors"] += 1
                    self._metrics["last_error"] = str(e)
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
