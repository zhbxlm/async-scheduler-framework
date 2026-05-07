"""Service dependency injection container.

All platform services are assembled here and injected into the FastAPI
app.state during lifespan startup. Never instantiate services outside
this container — it ensures singleton lifecycle and testability.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any

from src.platform.circuit_breaker import CircuitBreaker


@dataclass
class ServiceContainer:
    redis_client: Any = None
    async_engine: Any = None
    async_session_factory: Any = None
    capability_registry: Any = None
    cluster_registry: Any = None
    node_registry: Any = None
    schedule_registry: Any = None
    tenant_registry: Any = None
    circuit_breaker: Any = None
    dag_loader: Any = None
    queue_manager: Any = None
    task_creator: Any = None
    task_reconciler: Any = None
    task_completion_node: Any = None
    cron_scheduler: Any = None
    compensation_service: Any = None
    callback_dispatcher: Any = None

    @classmethod
    async def build_task_api(cls, settings) -> "ServiceContainer":
        from src.common.redis_client import create_redis_client
        from src.common.async_db import init_async_engine, get_async_db
        from src.platform.tenant_registry import TenantRegistry
        from src.platform.schedule_registry import ScheduleRegistry
        from src.platform.queue_manager import QueueManager
        from src.platform.task_creator import TaskCreator
        from src.platform.task_reconciler import TaskReconciler
        from src.platform.task_completion_node import TaskCompletionNode
        from src.platform.cron_scheduler import CronScheduler
        from src.services.compensation import CompensationService
        from src.services.callback_dispatcher import CallbackDispatchService
        from functools import partial

        c = cls()
        redis_url = str(settings.redis.url) if settings.redis.url else None
        c.redis_client = await create_redis_client(redis_url)
        db_factory = None
        if settings.mysql.url:
            engine, session_factory = init_async_engine(settings.mysql.url)
            c.async_engine = engine
            c.async_session_factory = session_factory
            db_factory = partial(get_async_db, session_factory)
        c.tenant_registry = TenantRegistry(c.redis_client)
        c.schedule_registry = ScheduleRegistry(c.redis_client)
        c.circuit_breaker = CircuitBreaker(redis_client=c.redis_client, key_prefix="queue:cb")
        c.queue_manager = QueueManager(c.redis_client, circuit_breaker=c.circuit_breaker)
        c.task_creator = TaskCreator(redis_client=c.redis_client, queue_manager=c.queue_manager, db_session_factory=db_factory)
        c.compensation_service = CompensationService(
            mysql_session_factory=db_factory,
            redis_client=c.redis_client,
            scan_interval_seconds=getattr(settings.background, 'compensation_interval', 60),
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
        c.task_completion_node = TaskCompletionNode(db_session_factory=db_factory, redis_client=c.redis_client)
        c.callback_dispatcher = CallbackDispatchService(db_factory) if db_factory else None
        ccfg = settings.background.cron
        cron_interval = getattr(ccfg, "poll_interval", 60)
        c.cron_scheduler = CronScheduler(redis_client=c.redis_client, schedule_repository=c.schedule_registry, task_creator=c.task_creator, poll_interval=float(cron_interval))
        return c

    @classmethod
    async def build_control_plane(cls, settings) -> "ServiceContainer":
        from src.common.redis_client import create_redis_client
        from src.common.async_db import init_async_engine, get_async_db
        from src.platform.schedule_registry import ScheduleRegistry
        from src.platform.queue_manager import QueueManager
        from src.platform.task_creator import TaskCreator
        from src.platform.task_reconciler import TaskReconciler
        from src.platform.task_completion_node import TaskCompletionNode
        from src.platform.cron_scheduler import CronScheduler
        from src.services.compensation import CompensationService
        from src.services.callback_dispatcher import CallbackDispatchService
        from functools import partial

        c = cls()
        redis_url = str(settings.redis.url) if settings.redis.url else None
        c.redis_client = await create_redis_client(redis_url)
        db_factory = None
        if settings.mysql.url:
            engine, session_factory = init_async_engine(settings.mysql.url)
            c.async_engine = engine
            c.async_session_factory = session_factory
            db_factory = partial(get_async_db, session_factory)
        c.schedule_registry = ScheduleRegistry(c.redis_client)
        c.circuit_breaker = CircuitBreaker(redis_client=c.redis_client, key_prefix="queue:cb")
        c.queue_manager = QueueManager(c.redis_client, circuit_breaker=c.circuit_breaker)
        c.task_creator = TaskCreator(redis_client=c.redis_client, queue_manager=c.queue_manager, db_session_factory=db_factory)
        c.compensation_service = CompensationService(mysql_session_factory=db_factory, redis_client=c.redis_client, scan_interval_seconds=getattr(settings.background, 'compensation_interval', 60))
        rcfg = settings.background.reconcile
        c.task_reconciler = TaskReconciler(redis_client=c.redis_client, db_session_factory=db_factory, queue_manager=c.queue_manager, interval_seconds=rcfg.interval_seconds, stuck_max_per_tick=rcfg.stuck_max_per_tick, stuck_task_max_age_seconds=rcfg.stuck_task_max_age_seconds, batch_size=rcfg.batch_size, compensation_service=c.compensation_service)
        c.task_completion_node = TaskCompletionNode(db_session_factory=db_factory, redis_client=c.redis_client)
        c.callback_dispatcher = CallbackDispatchService(db_factory) if db_factory else None
        ccfg = settings.background.cron
        cron_interval = getattr(ccfg, "poll_interval", 60)
        c.cron_scheduler = CronScheduler(redis_client=c.redis_client, schedule_repository=c.schedule_registry, task_creator=c.task_creator, poll_interval=float(cron_interval))
        return c

    @classmethod
    async def build_ops_api(cls, settings) -> "ServiceContainer":
        from src.common.redis_client import create_redis_client
        from src.platform.capability_registry import CapabilityRegistry
        from src.platform.cluster_registry import ClusterRegistry
        from src.platform.node_registry import NodeRegistry
        from src.platform.schedule_registry import ScheduleRegistry
        from src.platform.tenant_registry import TenantRegistry
        from src.platform.dag_loader import DagLoader
        from src.platform.queue_manager import QueueManager

        c = cls()
        redis_url = str(settings.redis.url) if settings.redis.url else None
        c.redis_client = await create_redis_client(redis_url)
        c.capability_registry = CapabilityRegistry(c.redis_client)
        c.cluster_registry = ClusterRegistry(c.redis_client)
        c.node_registry = NodeRegistry(c.redis_client)
        c.schedule_registry = ScheduleRegistry(c.redis_client)
        c.tenant_registry = TenantRegistry(c.redis_client)
        c.circuit_breaker = CircuitBreaker(redis_client=c.redis_client, key_prefix="queue:cb")
        c.dag_loader = DagLoader(redis_client=c.redis_client)
        c.queue_manager = QueueManager(c.redis_client, circuit_breaker=c.circuit_breaker)
        return c

    async def close(self) -> None:
        from src.common.redis_client import close_redis_client
        if self.callback_dispatcher:
            await self.callback_dispatcher.stop()
            self.callback_dispatcher = None
        if self.compensation_service:
            await self.compensation_service.stop()
            self.compensation_service = None
        if self.redis_client:
            await close_redis_client(self.redis_client)
            self.redis_client = None


_container: Optional[ServiceContainer] = None


def get_container() -> ServiceContainer:
    if _container is None:
        raise RuntimeError("ServiceContainer is not initialised. Ensure the FastAPI lifespan has started.")
    return _container


def set_container(c: ServiceContainer) -> None:
    global _container
    _container = c
