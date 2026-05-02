"""Service composition helpers.

This module provides a factory for creating the service container that wires
together all scheduler components. The service container now supports
pluggable backends for distributed scheduler support (Batch 1 of the
deepwiki distributed-alignment roadmap).

Batch 3 enhancements:
- Integrated enhanced TaskCompletionNode with TaskReconciler
- Integrated enhanced StepExecutors with DAGEngine
- Integrated enhanced CapabilityRegistry with handlers
"""

from __future__ import annotations

from dataclasses import dataclass

from async_scheduler.backends import BackendConfig, BackendFactory
from async_scheduler.backends.base import LockBackend
from async_scheduler.core.consumer import TaskConsumer
from async_scheduler.dag import DAGEngine, StepExecutors
from async_scheduler.distributed import WorkerRegistry
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
from async_scheduler.worker import WorkerPool, create_default_workers


@dataclass(frozen=True)
class DistributedSettings:
    redis_url: str
    lease_ttl_seconds: float
    heartbeat_interval_seconds: float


@dataclass
class ServiceContainer:
    queue_manager: QueueManager
    lock_backend: LockBackend | None
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
    step_executors: StepExecutors
    distributed_settings: DistributedSettings | None = None
    worker_registry: WorkerRegistry | None = None


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
    """Build the service container with optional backend configuration."""
    distributed_settings: DistributedSettings | None = None
    worker_registry: WorkerRegistry | None = None
    lock_backend: LockBackend | None = None

    if backend_config is not None:
        factory = BackendFactory(backend_config)
        lock_backend = factory.create_lock_backend()
        if backend_config.distributed_mode:
            distributed_settings = DistributedSettings(
                redis_url=backend_config.redis_url or "",
                lease_ttl_seconds=backend_config.lease_ttl_seconds,
                heartbeat_interval_seconds=backend_config.heartbeat_interval_seconds,
            )
            queue_backend = factory.create_queue_backend()
            worker_registry = WorkerRegistry(
                redis_url=distributed_settings.redis_url,
                heartbeat_ttl_seconds=distributed_settings.heartbeat_interval_seconds * 2,
            )
        else:
            queue_backend = factory.create_queue_backend()
            worker_registry = WorkerRegistry(redis_url=backend_config.redis_url or "redis://localhost:6379/0")
    else:
        factory = BackendFactory()
        queue_backend = factory.create_queue_backend()
        lock_backend = factory.create_lock_backend()
        worker_registry = WorkerRegistry(redis_url="redis://localhost:6379/0")

    step_executors = StepExecutors(enable_metrics=True)
    callback_dispatcher = CallbackDispatcher()
    completion_node = TaskCompletionNode(callback_dispatcher, enable_metrics=True)
    registry = await _build_default_registry()

    queue_manager = QueueManager(backend=queue_backend)
    dag_engine = DAGEngine(step_executors=step_executors)

    task_executor = TaskExecutor()
    quota_manager = TenantQuotaManager()
    task_router = TaskRouter(queue_manager, quota_manager=quota_manager)

    task_handler = RegistryTaskHandler(registry)
    dag_handler = RegistryDagHandler(registry)
    reconciler = TaskReconciler(completion_node=completion_node, lock_backend=lock_backend, worker_registry=worker_registry)

    task_consumer = TaskConsumer(
        queue_manager=queue_manager,
        executor=task_executor,
        handler=task_handler,
        max_concurrent_tasks=10,
        poll_interval=1.0,
        quota_manager=quota_manager,
        completion_node=completion_node,
        lock_backend=lock_backend,
    )

    cron_scheduler = CronScheduler(
        queue_manager=queue_manager,
        poll_interval=60.0,
    )

    worker_pool = WorkerPool(create_default_workers(queue_manager, task_executor, num_workers=1))

    return ServiceContainer(
        queue_manager=queue_manager,
        lock_backend=lock_backend,
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
        step_executors=step_executors,
        distributed_settings=distributed_settings,
        worker_registry=worker_registry,
    )
