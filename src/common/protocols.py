"""Typed protocol contracts for platform services.

Defines structural interfaces (Protocol classes) that platform services
must satisfy. This enforces the dependency-inversion principle:
high-level modules (routes, container) depend on abstractions, not
concrete implementations.

Usage:
    from src.common.protocols import IQueueManager

    def setup(queue: IQueueManager) -> None:
        ...  # works with any conforming implementation
"""
from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class IQueueManager(Protocol):
    """Queue operations contract."""
    async def enqueue(self, capability: str, task_id: str, priority: int = 3,
                      execute_after_ms: int = 0) -> dict: ...
    async def dequeue(self, capability: str) -> dict | None: ...
    async def get_queue_depth(self, capability: str) -> int: ...


@runtime_checkable
class ITaskCreator(Protocol):
    """Task creation contract."""
    async def create_task(
        self, capability: str, dag_id: str, priority: str,
        input_data: dict, tenant_id: str = "",
        idempotency_key: str | None = None,
        **kwargs,
    ) -> dict: ...


@runtime_checkable
class ICapabilityRegistry(Protocol):
    """Capability registry contract."""
    async def register(self, cap: Any) -> None: ...
    async def get(self, name: str) -> Any | None: ...
    async def list_all(self) -> list[Any]: ...
    async def unregister(self, name: str) -> None: ...
    async def update_health(self, name: str, success: bool, **kwargs) -> Any: ...


@runtime_checkable
class INodeRegistry(Protocol):
    """Node registry contract."""
    async def register(self, node_info: Any) -> None: ...
    async def get(self, node_id: str) -> Any | None: ...
    async def heartbeat(self, node_id: str, resources: dict | None = None) -> None: ...
    async def list_all(self) -> list[Any]: ...
    async def detect_dead_nodes(self, timeout_seconds: float = 60.0) -> list[str]: ...


@runtime_checkable
class IClusterRegistry(Protocol):
    """Cluster registry contract."""
    async def register(self, cluster: Any) -> None: ...
    async def get(self, cluster_id: str) -> Any | None: ...
    async def list_all(self) -> list[Any]: ...
    async def select_cluster(
        self, required_gpus: int = 0,
        capability: str | None = None,
        tenant_id: str | None = None,
    ) -> Any | None: ...


@runtime_checkable
class ITenantRegistry(Protocol):
    """Tenant registry contract."""
    async def validate_key(self, api_key: str, tenant_id: str | None) -> dict | None: ...
    async def get_tenant(self, tenant_id: str) -> Any | None: ...


@runtime_checkable
class IDagLoader(Protocol):
    """DAG definition loader contract."""
    def get(self, dag_id: str) -> Any | None: ...
    def list_all(self) -> list[str]: ...
