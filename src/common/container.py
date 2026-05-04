"""Service dependency injection container.

All platform services are assembled here and injected into the FastAPI
app.state during lifespan startup. Never instantiate services outside
this container — it ensures singleton lifecycle and testability.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any


@dataclass
class ServiceContainer:
    """Assembles and holds all platform service singletons.

    Use ``ServiceContainer.build(settings)`` as the factory.
    Callers access services via ``get_container()`` or directly
    through ``request.app.state.*`` in route handlers.
    """
    redis_client: Any = None

    # ── Registries ────────────────────────────────────────────────
    capability_registry: Any = None
    cluster_registry: Any = None
    node_registry: Any = None
    schedule_registry: Any = None
    tenant_registry: Any = None

    # ── Platform services ─────────────────────────────────────────
    dag_loader: Any = None
    queue_manager: Any = None
    task_creator: Any = None
    task_reconciler: Any = None
    cron_scheduler: Any = None

    @classmethod
    async def build(cls, settings) -> "ServiceContainer":
        """Build all services from configuration.

        This is the single place where service wiring happens.
        """
        from src.common.redis_client import create_redis_client
        from src.common.async_db import init_async_engine, get_async_db
        from src.platform.capability_registry import CapabilityRegistry
        from src.platform.cluster_registry import ClusterRegistry
        from src.platform.node_registry import NodeRegistry
        from src.platform.schedule_registry import ScheduleRegistry
        from src.platform.tenant_registry import TenantRegistry
        from src.platform.dag_loader import DagLoader
        from src.platform.queue_manager import QueueManager
        from src.platform.task_creator import TaskCreator
        from src.platform.task_reconciler import TaskReconciler
        from src.platform.cron_scheduler import CronScheduler

        c = cls()
        c.redis_client = create_redis_client(settings.redis.url or None)

        if settings.mysql.url:
            init_async_engine(settings.mysql.url)

        # Registries
        c.capability_registry = CapabilityRegistry(c.redis_client)
        c.cluster_registry = ClusterRegistry(c.redis_client)
        c.node_registry = NodeRegistry(c.redis_client)
        c.schedule_registry = ScheduleRegistry(c.redis_client)
        c.tenant_registry = TenantRegistry(c.redis_client)

        # Core platform
        c.dag_loader = DagLoader(redis_client=c.redis_client)
        c.queue_manager = QueueManager(c.redis_client)

        db_factory = get_async_db if settings.mysql.url else None

        c.task_creator = TaskCreator(
            redis_client=c.redis_client,
            queue_manager=c.queue_manager,
            db_session_factory=db_factory,
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
        )

        ccfg = settings.background.cron
        cron_interval = getattr(ccfg, "poll_interval", 60)

        c.cron_scheduler = CronScheduler(
            redis_client=c.redis_client,
            schedule_repository=c.schedule_registry,
            task_creator=c.task_creator,
            poll_interval=float(cron_interval),
        )

        return c


# ── Global singleton ──────────────────────────────────────────────────────
_container: Optional[ServiceContainer] = None


def get_container() -> ServiceContainer:
    """Return the application-level service container.

    Raises RuntimeError if called before lifespan startup.
    """
    if _container is None:
        raise RuntimeError(
            "ServiceContainer is not initialised. "
            "Ensure the FastAPI lifespan has started."
        )
    return _container


def set_container(c: ServiceContainer) -> None:
    """Set the global container (called once during lifespan startup)."""
    global _container
    _container = c
