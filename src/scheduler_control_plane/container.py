"""Package-owned control-plane container composition."""
from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import Any

from src.common.lifecycle import LifecycleManager
from src.platform.circuit_breaker import CircuitBreaker
from src.platform.queue_manager import QueueManager
from src.platform.schedule_registry import ScheduleRegistry
from src.platform.task_completion_node import TaskCompletionNode
from src.platform.task_creator import TaskCreator
from src.platform.tenant_registry import TenantRegistry


@dataclass
class ControlPlaneContainer:
    redis_client: Any = None
    async_engine: Any = None
    async_session_factory: Any = None
    schedule_registry: Any = None
    tenant_registry: Any = None
    circuit_breaker: Any = None
    queue_manager: Any = None
    task_creator: Any = None
    task_completion_node: Any = None
    task_reconciler: Any = None
    cron_scheduler: Any = None
    compensation_service: Any = None
    callback_dispatcher: Any = None
    lifecycle_manager: LifecycleManager | None = None


def set_container(container: Any) -> None:
    pass  # standalone: no monorepo compat


async def build_container(settings: Any) -> ControlPlaneContainer:
    from src.common.async_db import get_async_db, init_async_engine
    from src.common.redis_client import create_redis_client
    from src.platform.cron_scheduler import CronScheduler
    from src.platform.task_reconciler import TaskReconciler
    from src.services.callback_dispatcher import CallbackDispatchService
    from src.services.compensation import CompensationService

    c = ControlPlaneContainer(lifecycle_manager=LifecycleManager())

    # Redis
    redis_url = str(settings.redis.url) if settings.redis.url else None
    c.redis_client = await create_redis_client(redis_url)

    # MySQL
    if settings.mysql.url:
        c.async_engine, c.async_session_factory = init_async_engine(settings.mysql.url)
        db_factory = partial(get_async_db, c.async_session_factory)
    else:
        db_factory = None

    # Registries
    c.schedule_registry = ScheduleRegistry(c.redis_client)
    c.tenant_registry = TenantRegistry(c.redis_client)

    # Queue stack
    c.circuit_breaker = CircuitBreaker(redis_client=c.redis_client, key_prefix="queue:cb")
    c.queue_manager = QueueManager(c.redis_client, circuit_breaker=c.circuit_breaker)

    # Task runtime
    c.task_creator = TaskCreator(
        redis_client=c.redis_client,
        queue_manager=c.queue_manager,
        db_session_factory=db_factory,
    )
    c.task_completion_node = TaskCompletionNode(
        db_session_factory=db_factory,
        redis_client=c.redis_client,
    )

    # Background services
    c.compensation_service = CompensationService(
        mysql_session_factory=db_factory,
        redis_client=c.redis_client,
        scan_interval_seconds=getattr(settings.background, "compensation_interval", 60),
    )

    rcfg = settings.background.reconcile
    c.task_reconciler = TaskReconciler(
        redis_client=c.redis_client,
        db_session_factory=db_factory,
        queue_manager=c.queue_manager,
        interval_seconds=rcfg.interval_seconds,
        stuck_max_per_tick=rcfg.stuck_max_per_tick,
        stuck_task_max_age_seconds=rcfg.stuck_task_max_age_seconds,
        batch_size=rcfg.batch_size,
        compensation_service=c.compensation_service,
    )

    c.callback_dispatcher = CallbackDispatchService(db_factory) if db_factory else None

    ccfg = settings.background.cron
    c.cron_scheduler = CronScheduler(
        redis_client=c.redis_client,
        schedule_repository=c.schedule_registry,
        task_creator=c.task_creator,
        poll_interval=float(getattr(ccfg, "poll_interval", 60)),
    )

    return c
