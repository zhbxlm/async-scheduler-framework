"""Platform services."""

from async_scheduler.platform.callback import CallbackDispatcher
from async_scheduler.platform.completion import CompletionMetrics, TaskCompletionNode
from async_scheduler.platform.handlers import RegistryDagHandler, RegistryTaskHandler
from async_scheduler.platform.quota import QuotaExceededError, TenantQuotaManager
from async_scheduler.platform.reconciler import (
    ReconciliationConfig,
    ReconciliationMetrics,
    RepairStrategy,
    TaskReconciler,
)
from async_scheduler.platform.router import TaskRouter

__all__ = [
    "TaskRouter",
    "CallbackDispatcher",
    "ServiceContainer",
    "build_service_container",
    "QuotaExceededError",
    "TenantQuotaManager",
    "RegistryTaskHandler",
    "RegistryDagHandler",
    "TaskCompletionNode",
    "CompletionMetrics",
    "TaskReconciler",
    "ReconciliationConfig",
    "ReconciliationMetrics",
    "RepairStrategy",
]


def __getattr__(name: str):
    if name in {"ServiceContainer", "build_service_container"}:
        from async_scheduler.platform.services import ServiceContainer, build_service_container

        exports = {
            "ServiceContainer": ServiceContainer,
            "build_service_container": build_service_container,
        }
        return exports[name]
    raise AttributeError(name)
