"""Package-owned ops-api container composition."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from scheduler_ops_api.infra.redis_client import create_redis_client
from scheduler_ops_api.infra.schedule_registry import ScheduleRegistry
from scheduler_ops_api.infra.tenant_registry import TenantRegistry
from scheduler_ops_api.lifecycle import LifecycleManager
from src.platform.circuit_breaker import CircuitBreaker
from src.platform.queue_manager import QueueManager


@dataclass
class OpsApiContainer:
    redis_client: Any = None
    capability_registry: Any = None
    cluster_registry: Any = None
    node_registry: Any = None
    schedule_registry: Any = None
    tenant_registry: Any = None
    dag_loader: Any = None
    circuit_breaker: Any = None
    queue_manager: Any = None
    lifecycle_manager: LifecycleManager | None = None


def _new_container() -> OpsApiContainer:
    return OpsApiContainer(lifecycle_manager=LifecycleManager())


async def _init_redis_client(settings) -> Any:
    redis_url = str(settings.redis.url) if settings.redis.url else None
    return await create_redis_client(redis_url)


def _init_queue_stack(container: OpsApiContainer) -> None:
    container.circuit_breaker = CircuitBreaker(redis_client=container.redis_client, key_prefix="queue:cb")
    container.queue_manager = QueueManager(container.redis_client, circuit_breaker=container.circuit_breaker)


def _init_schedule_registry(container: OpsApiContainer) -> None:
    container.schedule_registry = ScheduleRegistry(container.redis_client)


def _init_tenant_registry(container: OpsApiContainer) -> None:
    container.tenant_registry = TenantRegistry(container.redis_client)


def set_container(container: Any) -> None:
    pass  # standalone: no monorepo compat


async def build_container(settings: Any) -> OpsApiContainer:
    from src.platform.capability_registry import CapabilityRegistry
    from src.platform.cluster_registry import ClusterRegistry
    from src.platform.dag_loader import DagLoader
    from src.platform.node_registry import NodeRegistry

    container = _new_container()
    container.redis_client = await _init_redis_client(settings)
    container.capability_registry = CapabilityRegistry(container.redis_client)
    container.cluster_registry = ClusterRegistry(container.redis_client)
    container.node_registry = NodeRegistry(container.redis_client)
    _init_schedule_registry(container)
    _init_tenant_registry(container)
    container.dag_loader = DagLoader(redis_client=container.redis_client)
    _init_queue_stack(container)
    set_container(container)
    return container
