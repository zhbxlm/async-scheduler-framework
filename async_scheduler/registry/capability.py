"""Capability registry for routing task types and DAG node types.

The CapabilityRegistry provides a central registry for task capabilities and
their associated handlers. This is a key platform component for routing
tasks to appropriate execution logic.

This is Batch 3 of the deepwiki distributed-alignment roadmap: improving
capability management API/metadata consistent with the local MVP.

The CapabilityRegistry provides:
- Registration and lookup of capability handlers
- Rich metadata for capabilities (description, version, schema, etc.)
- Capability discovery and enumeration
- Validation and schema checking (planned)
- Integration with the platform service container
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any, TypeAlias

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

CapabilityHandler: TypeAlias = Callable[[dict[str, Any]], Awaitable[Any]]


class CapabilityInfo(BaseModel):
    """Metadata for a registered capability."""
    name: str
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    version: str = "1.0.0"
    author: str = ""
    schema_: dict[str, Any] | None = Field(default=None, alias="schema")
    created_at: str | None = None
    updated_at: str | None = None
    enabled: bool = True
    execution_count: int = 0
    last_executed_at: str | None = None


class CapabilityRegistry:
    """Registry for task capability handlers with rich metadata.

    The CapabilityRegistry serves as the central dispatch point for task
    execution. Tasks specify a capability name, and the registry routes
    them to the appropriate handler.

    This is Batch 3 of the deepwiki distributed-alignment roadmap:
    improving capability management with better metadata and
    lifecycle control.

    Args:
        enable_metrics: Whether to track execution metrics (default: True).
    """

    def __init__(self, enable_metrics: bool = True) -> None:
        """Initialize the capability registry."""
        self._handlers: dict[str, CapabilityHandler] = {}
        self._metadata: dict[str, CapabilityInfo] = {}
        self._enable_metrics = enable_metrics

    def register(
        self,
        capability: str,
        handler: CapabilityHandler,
        *,
        description: str = "",
        tags: list[str] | None = None,
        version: str = "1.0.0",
        author: str = "",
        schema: dict[str, Any] | None = None,
        enabled: bool = True,
    ) -> None:
        """Register a capability with its handler and metadata.

        Args:
            capability: The capability name.
            handler: The async handler function.
            description: Human-readable description.
            tags: List of tags for categorization.
            version: Capability version.
            author: Capability author/maintainer.
            schema: Optional JSON schema for payload validation.
            enabled: Whether the capability is enabled.
        """
        now = datetime.utcnow().isoformat()

        # Update or create metadata
        if capability in self._metadata:
            info = self._metadata[capability]
            info.description = description or info.description
            info.tags = tags or info.tags
            info.version = version
            info.author = author or info.author
            info.schema_ = schema or info.schema_
            info.updated_at = now
            info.enabled = enabled
        else:
            self._metadata[capability] = CapabilityInfo(
                name=capability,
                description=description,
                tags=tags or [],
                version=version,
                author=author,
                schema=schema,
                created_at=now,
                updated_at=now,
                enabled=enabled,
            )

        self._handlers[capability] = handler
        logger.info(f"Registered capability: {capability} (v{version}, enabled={enabled})")

    def unregister(self, capability: str) -> bool:
        """Unregister a capability.

        Args:
            capability: The capability name to unregister.

        Returns:
            True if unregistered, False if not found.
        """
        if capability in self._handlers:
            del self._handlers[capability]
            del self._metadata[capability]
            logger.info(f"Unregistered capability: {capability}")
            return True
        return False

    def get(self, capability: str) -> CapabilityHandler | None:
        """Get a capability handler.

        Args:
            capability: The capability name.

        Returns:
            The handler, or None if not found or disabled.
        """
        info = self._metadata.get(capability)
        if info is None:
            return None
        if not info.enabled:
            logger.warning(f"Attempted to use disabled capability: {capability}")
            return None
        return self._handlers.get(capability)

    def get_info(self, capability: str) -> CapabilityInfo | None:
        """Get capability metadata.

        Args:
            capability: The capability name.

        Returns:
            The CapabilityInfo, or None if not found.
        """
        return self._metadata.get(capability)

    def list_capabilities(self, include_disabled: bool = False) -> list[str]:
        """List all registered capability names.

        Args:
            include_disabled: Whether to include disabled capabilities.

        Returns:
            Sorted list of capability names.
        """
        if include_disabled:
            return sorted(self._handlers.keys())
        return sorted(k for k, v in self._metadata.items() if v.enabled)

    def list_capability_info(self, include_disabled: bool = False) -> list[CapabilityInfo]:
        """List all capability metadata.

        Args:
            include_disabled: Whether to include disabled capabilities.

        Returns:
            List of CapabilityInfo objects.
        """
        if include_disabled:
            return [self._metadata[name] for name in sorted(self._metadata.keys())]
        return [v for v in self._metadata.values() if v.enabled]

    def find_by_tag(self, tag: str) -> list[CapabilityInfo]:
        """Find capabilities by tag.

        Args:
            tag: The tag to search for.

        Returns:
            List of CapabilityInfo objects with the tag.
        """
        return [info for info in self._metadata.values() if tag in info.tags]

    def enable(self, capability: str) -> bool:
        """Enable a capability.

        Args:
            capability: The capability name.

        Returns:
            True if enabled, False if not found.
        """
        if capability in self._metadata:
            self._metadata[capability].enabled = True
            return True
        return False

    def disable(self, capability: str) -> bool:
        """Disable a capability.

        Args:
            capability: The capability name.

        Returns:
            True if disabled, False if not found.
        """
        if capability in self._metadata:
            self._metadata[capability].enabled = False
            return True
        return False

    async def dispatch(self, capability: str, payload: dict[str, Any]) -> Any:
        """Dispatch a task to its capability handler.

        Args:
            capability: The capability name.
            payload: The task payload.

        Returns:
            The result from the handler.

        Raises:
            KeyError: If the capability is not found or disabled.
        """
        handler = self.get(capability)
        if handler is None:
            raise KeyError(f"Capability not found or disabled: {capability}")

        # Update metrics
        if self._enable_metrics and capability in self._metadata:
            info = self._metadata[capability]
            info.execution_count += 1
            info.last_executed_at = datetime.utcnow().isoformat()

        return await handler(payload)

    def get_metrics(self) -> dict[str, Any]:
        """Get registry-wide metrics.

        Returns:
            Dictionary with metrics data.
        """
        return {
            "total_capabilities": len(self._handlers),
            "enabled_capabilities": sum(1 for v in self._metadata.values() if v.enabled),
            "disabled_capabilities": sum(1 for v in self._metadata.values() if not v.enabled),
            "total_executions": sum(v.execution_count for v in self._metadata.values()),
            "tags": self._get_all_tags(),
        }

    def _get_all_tags(self) -> list[str]:
        """Get all unique tags across all capabilities."""
        tags = set()
        for info in self._metadata.values():
            tags.update(info.tags)
        return sorted(tags)
