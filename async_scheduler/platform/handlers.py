"""Execution handlers bound to service registry."""

from __future__ import annotations

from typing import Any

from async_scheduler.registry import CapabilityRegistry


class RegistryTaskHandler:
    """Dispatch plain task payloads using capability registry."""

    def __init__(self, registry: CapabilityRegistry, default_capability: str = "default") -> None:
        self.registry = registry
        self.default_capability = default_capability

    async def __call__(self, payload: dict[str, Any]) -> Any:
        capability = payload.get("capability", self.default_capability)
        return await self.registry.dispatch(capability, payload)


class RegistryDagHandler:
    """Dispatch DAG node task types using capability registry."""

    def __init__(self, registry: CapabilityRegistry) -> None:
        self.registry = registry

    async def __call__(self, task_type: str, payload: dict[str, Any]) -> Any:
        return await self.registry.dispatch(task_type, payload)
