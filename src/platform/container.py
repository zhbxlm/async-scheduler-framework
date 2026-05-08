"""Service dependency injection container.

All platform services are assembled here and injected into the FastAPI
app.state during lifespan startup. Never instantiate services outside
this container — it ensures singleton lifecycle and testability.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from src.common.lifecycle import LifecycleManager
from src.platform.circuit_breaker import CircuitBreaker


def _new_container() -> "ServiceContainer":
    return ServiceContainer(lifecycle_manager=LifecycleManager())


async def _init_redis_client(settings) -> Any:
    from src.common.redis_client import create_redis_client

    redis_url = str(settings.redis.url) if settings.redis.url else None
    return await create_redis_client(redis_url)


def _init_async_db(settings) -> tuple[Any, Any, Any]:
    from functools import partial

    from src.common.async_db import get_async_db, init_async_engine

    if not settings.mysql.url:
        return None, None, None

    engine, session_factory = init_async_engine(settings.mysql.url)
    db_factory = partial(get_async_db, session_factory)
    return engine, session_factory, db_factory


def _init_queue_stack(container: "ServiceContainer") -> None:
    from src.platform.queue_manager import QueueManager

    container.circuit_breaker = CircuitBreaker(redis_client=container.redis_client, key_prefix="queue:cb")
    container.queue_manager = QueueManager(container.redis_client, circuit_breaker=container.circuit_breaker)


def _init_schedule_registry(container: "ServiceContainer") -> None:
    from src.platform.schedule_registry import ScheduleRegistry

    container.schedule_registry = ScheduleRegistry(container.redis_client)


def _init_tenant_registry(container: "ServiceContainer") -> None:
    from src.platform.tenant_registry import TenantRegistry

    container.tenant_registry = TenantRegistry(container.redis_client)


def _init_task_runtime_components(container: "ServiceContainer", db_factory: Any) -> None:
    from src.platform.task_completion_node import TaskCompletionNode
    from src.platform.task_creator import TaskCreator

    container.task_creator = TaskCreator(
        redis_client=container.redis_client,
        queue_manager=container.queue_manager,
        db_session_factory=db_factory,
    )
    container.task_completion_node = TaskCompletionNode(
        db_session_factory=db_factory,
        redis_client=container.redis_client,
    )


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
    lifecycle_manager: LifecycleManager | None = None

    @classmethod
    async def build_task_api(cls, settings) -> "ServiceContainer":
        c = _new_container()
        c.redis_client = await _init_redis_client(settings)
        c.async_engine, c.async_session_factory, db_factory = _init_async_db(settings)
        _init_tenant_registry(c)
        _init_schedule_registry(c)
        _init_queue_stack(c)
        _init_task_runtime_components(c, db_factory)
        return c

    @classmethod
    async def build_control_plane(cls, settings) -> "ServiceContainer":
        from src.platform.cron_scheduler import CronScheduler
        from src.platform.task_reconciler import TaskReconciler
        from src.services.callback_dispatcher import CallbackDispatchService
        from src.services.compensation import CompensationService

        c = _new_container()
        c.redis_client = await _init_redis_client(settings)
        c.async_engine, c.async_session_factory, db_factory = _init_async_db(settings)
        _init_schedule_registry(c)
        _init_queue_stack(c)
        _init_task_runtime_components(c, db_factory)
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
        cron_interval = getattr(ccfg, "poll_interval", 60)
        c.cron_scheduler = CronScheduler(
            redis_client=c.redis_client,
            schedule_repository=c.schedule_registry,
            task_creator=c.task_creator,
            poll_interval=float(cron_interval),
        )
        return c

    @classmethod
    async def build_ops_api(cls, settings) -> "ServiceContainer":
        from src.platform.capability_registry import CapabilityRegistry
        from src.platform.cluster_registry import ClusterRegistry
        from src.platform.dag_loader import DagLoader
        from src.platform.node_registry import NodeRegistry

        c = _new_container()
        c.redis_client = await _init_redis_client(settings)
        c.capability_registry = CapabilityRegistry(c.redis_client)
        c.cluster_registry = ClusterRegistry(c.redis_client)
        c.node_registry = NodeRegistry(c.redis_client)
        _init_schedule_registry(c)
        _init_tenant_registry(c)
        c.dag_loader = DagLoader(redis_client=c.redis_client)
        _init_queue_stack(c)
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
