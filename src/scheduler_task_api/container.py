"""Package-owned task-api container composition."""
from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import Any

from scheduler_task_api.lifecycle import LifecycleManager
from src.platform.circuit_breaker import CircuitBreaker


@dataclass
class TaskApiContainer:
    redis_client: Any = None
    async_engine: Any = None
    async_session_factory: Any = None
    tenant_registry: Any = None
    schedule_registry: Any = None
    circuit_breaker: Any = None
    queue_manager: Any = None
    task_creator: Any = None
    task_completion_node: Any = None
    lifecycle_manager: LifecycleManager | None = None


def _new_container() -> TaskApiContainer:
    return TaskApiContainer(lifecycle_manager=LifecycleManager())


async def _init_redis_client(settings) -> Any:
    from scheduler_task_api.infra.redis_client import create_redis_client

    redis_url = str(settings.redis.url) if settings.redis.url else None
    return await create_redis_client(redis_url)


def _init_async_db(settings) -> tuple[Any, Any, Any]:
    from scheduler_task_api.async_db import get_async_db, init_async_engine

    if not settings.mysql.url:
        return None, None, None

    engine, session_factory = init_async_engine(settings.mysql.url)
    db_factory = partial(get_async_db, session_factory)
    return engine, session_factory, db_factory


def _init_tenant_registry(container: TaskApiContainer) -> None:
    from scheduler_task_api.infra.tenant_registry import TenantRegistry

    container.tenant_registry = TenantRegistry(container.redis_client)


def _init_schedule_registry(container: TaskApiContainer) -> None:
    from scheduler_task_api.infra.schedule_registry import ScheduleRegistry

    container.schedule_registry = ScheduleRegistry(container.redis_client)


def _init_queue_stack(container: TaskApiContainer) -> None:
    from src.platform.queue_manager import QueueManager

    container.circuit_breaker = CircuitBreaker(redis_client=container.redis_client, key_prefix="queue:cb")
    container.queue_manager = QueueManager(container.redis_client, circuit_breaker=container.circuit_breaker)


def _init_task_runtime_components(container: TaskApiContainer, db_factory: Any) -> None:
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


def set_container(container: Any) -> None:
    pass  # standalone: monorepo compat removed


async def build_container(settings: Any) -> TaskApiContainer:
    container = _new_container()
    container.redis_client = await _init_redis_client(settings)
    container.async_engine, container.async_session_factory, db_factory = _init_async_db(settings)
    _init_tenant_registry(container)
    _init_schedule_registry(container)
    _init_queue_stack(container)
    _init_task_runtime_components(container, db_factory)
    set_container(container)
    return container
