"""Capability registry for routing task types and DAG node types."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import BaseModel, Field

CapabilityHandler = Callable[[dict[str, Any]], Awaitable[Any]]


class CapabilityInfo(BaseModel):
    name: str
    description: str = ""
    tags: list[str] = Field(default_factory=list)


class CapabilityRegistry:
    """Simple in-process capability registry."""

    def __init__(self) -> None:
        self._handlers: dict[str, CapabilityHandler] = {}
        self._metadata: dict[str, CapabilityInfo] = {}

    def register(
        self,
        capability: str,
        handler: CapabilityHandler,
        *,
        description: str = "",
        tags: list[str] | None = None,
    ) -> None:
        self._handlers[capability] = handler
        self._metadata[capability] = CapabilityInfo(
            name=capability,
            description=description,
            tags=tags or [],
        )

    def unregister(self, capability: str) -> None:
        self._handlers.pop(capability, None)
        self._metadata.pop(capability, None)

    def get(self, capability: str) -> CapabilityHandler | None:
        return self._handlers.get(capability)

    def get_info(self, capability: str) -> CapabilityInfo | None:
        return self._metadata.get(capability)

    def list_capabilities(self) -> list[str]:
        return sorted(self._handlers.keys())

    def list_capability_info(self) -> list[CapabilityInfo]:
        return [self._metadata[name] for name in sorted(self._metadata.keys())]

    async def dispatch(self, capability: str, payload: dict[str, Any]) -> Any:
        handler = self.get(capability)
        if handler is None:
            raise KeyError(f"Capability not registered: {capability}")
        return await handler(payload)
