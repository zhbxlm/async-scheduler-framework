"""Service composition helpers.

This module provides a factory for creating the service container that wires
together all scheduler components. The service container now supports
pluggable backends for distributed scheduler support (Batch 1 of the
deepwiki distributed-alignment roadmap).
"""

from __future__ import annotations

from dataclasses import dataclass

from async_scheduler.backends import BackendConfig, BackendFactory, QueueBackend
from async_scheduler.core.consumer import TaskConsumer
from async_scheduler.dag import DAGEngine
from async_scheduler.executor import TaskExecutor, default_task_handler
from async_scheduler.platform.callback import CallbackDispatcher
from async_scheduler.platform.completion import TaskCompletionNode
from async_scheduler.platform.handlers import RegistryDagHandler, RegistryTaskHandler
from async_scheduler.platform.quota import TenantQuotaManager
from async_scheduler.platform.reconciler import TaskReconciler
from async_scheduler.platform.router import TaskRouter
from async_scheduler.queue import QueueManager
from async_scheduler.registry import CapabilityRegistry
from async_scheduler.scheduler import CronScheduler
from async_scheduler.worker import create_default_workers, WorkerPool


@dataclass
class ServiceContainer:
    queue_manager: QueueManager
    task_executor: TaskExecutor
    dag_engine: DAGEngine
    task_router: TaskRouter
    callback_dispatcher: CallbackDispatcher
    completion_node: TaskCompletionNode
    quota_manager: TenantQuotaManager
    registry: CapabilityRegistry
    task_consumer: TaskConsumer
    cron_scheduler: CronScheduler
    worker_pool: WorkerPool
    task_handler: RegistryTaskHandler
    dag_handler: RegistryDagHandler
    reconciler: TaskReconciler


async def _build_default_registry() -> CapabilityRegistry:
    registry = CapabilityRegistry()
    registry.register("default", default_task_handler, description="Default async task handler", tags=["default"])
    registry.register("echo", default_task_handler, description="Echo capability", tags=["utility"])
    registry.register("compute", default_task_handler, description="Compute capability", tags=["math"])
    registry.register("io", default_task_handler, description="I/O capability", tags=["io"])
    registry.register("email", default_task_handler, description="Email capability", tags=["notify"])
    return registry


async def build_service_container(
    backend_config: BackendConfig | None = None,
) -> ServiceContainer:
    """Build the service container with optional backend configuration.

    This factory function creates all the services needed for the scheduler,
    optionally using configured backends for queue, lock, and registry operations.

    Args:
        backend_config: Optional backend configuration. If None, uses default
            in-memory backends (maintaining current behavior).

    Returns:
        A fully configured ServiceContainer instance.
    """
    # Create backends if config is provided, otherwise use defaults
    if backend_config is not None:
        factory = BackendFactory(backend_config)
        queue_backend = factory.create_queue_backend()
        # Note: Lock backend is available but not yet wired into the core flow
        # This is prepared for future distributed use cases
        # lock_backend = factory.create_lock_backend()
        # Note: Registry backend is currently created by ScheduleRegistry directly
        # This is prepared for future configuration flexibility
    else:
        # Use default in-memory backend for queue
        queue_backend = None  # QueueManager will create InMemoryQueueBackend

    queue_manager = QueueManager(backend=queue_backend)
    task_executor = TaskExecutor()
    dag_engine = DAGEngine()
    quota_manager = TenantQuotaManager()
    task_router = TaskRouter(queue_manager, quota_manager=quota_manager)
    callback_dispatcher = CallbackDispatcher()
    completion_node = TaskCompletionNode(callback_dispatcher)
    registry = await _build_default_registry()
    reconciler = TaskReconciler()

    task_handler = RegistryTaskHandler(registry)
    dag_handler = RegistryDagHandler(registry)

    task_consumer = TaskConsumer(
        queue_manager=queue_manager,
        executor=task_executor,
        handler=task_handler,
        max_concurrent_tasks=10,
        poll_interval=1.0,
        quota_manager=quota_manager,
        completion_node=completion_node,
    )

    cron_scheduler = CronScheduler(
        queue_manager=queue_manager,
        poll_interval=60.0,
    )

    worker_pool = WorkerPool(create_default_workers(queue_manager, task_executor, num_workers=1))

    return ServiceContainer(
        queue_manager=queue_manager,
        task_executor=task_executor,
        dag_engine=dag_engine,
        task_router=task_router,
        callback_dispatcher=callback_dispatcher,
        completion_node=completion_node,
        quota_manager=quota_manager,
        registry=registry,
        task_consumer=task_consumer,
        cron_scheduler=cron_scheduler,
        worker_pool=worker_pool,
        task_handler=task_handler,
        dag_handler=dag_handler,
        reconciler=reconciler,
    )
