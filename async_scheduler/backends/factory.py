"""Factory for creating backend instances.

The factory pattern allows for easy configuration and instantiation of
different backend implementations without requiring changes to the
core scheduler code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from async_scheduler.backends.base import LockBackend, QueueBackend, RegistryBackend
from async_scheduler.backends.memory import (
    InMemoryLockBackend,
    InMemoryQueueBackend,
    InMemoryRegistryBackend,
)


@dataclass
class BackendConfig:
    """Configuration for backend selection and initialization.

    This configuration allows users to select different backend implementations
    without modifying the scheduler code. Future versions will support additional
    backend types like "redis", "postgresql", etc.

    Attributes:
        queue_type: Type of queue backend ("memory" for in-memory, "redis" for Redis, etc.)
        lock_type: Type of lock backend ("memory" for in-memory, "redis" for Redis, etc.)
        registry_type: Type of registry backend ("memory" for in-memory/SQLite, etc.)
        queue_config: Additional configuration for queue backend.
        lock_config: Additional configuration for lock backend.
        registry_config: Additional configuration for registry backend.
    """

    queue_type: str = "memory"
    lock_type: str = "memory"
    registry_type: str = "memory"
    queue_config: dict[str, Any] = field(default_factory=dict)
    lock_config: dict[str, Any] = field(default_factory=dict)
    registry_config: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def default(cls) -> BackendConfig:
        """Create default configuration with in-memory backends."""
        return cls()


class BackendFactory:
    """Factory for creating backend instances.

    This factory provides a single point of configuration for all backend
    implementations, making it easy to switch between different storage
    backends (in-memory, Redis, etc.) without changing application code.
    """

    _config: BackendConfig

    def __init__(self, config: BackendConfig | None = None) -> None:
        """Initialize the factory with a configuration.

        Args:
            config: Backend configuration. Defaults to in-memory backends.
        """
        self._config = config or BackendConfig.default()

    def create_queue_backend(self) -> QueueBackend:
        """Create a queue backend instance based on configuration.

        Returns:
            A configured QueueBackend instance.
        """
        backend_type = self._config.queue_type

        if backend_type == "memory":
            return InMemoryQueueBackend()

        # Future implementations:
        # elif backend_type == "redis":
        #     return RedisQueueBackend(**self._config.queue_config)
        # elif backend_type == "rabbitmq":
        #     return RabbitMQQueueBackend(**self._config.queue_config)

        raise ValueError(f"Unknown queue backend type: {backend_type}")

    def create_lock_backend(self) -> LockBackend:
        """Create a lock backend instance based on configuration.

        Returns:
            A configured LockBackend instance.
        """
        backend_type = self._config.lock_type

        if backend_type == "memory":
            return InMemoryLockBackend()

        # Future implementations:
        # elif backend_type == "redis":
        #     return RedisLockBackend(**self._config.lock_config)

        raise ValueError(f"Unknown lock backend type: {backend_type}")

    def create_registry_backend(self) -> RegistryBackend:
        """Create a registry backend instance based on configuration.

        Returns:
            A configured RegistryBackend instance.
        """
        backend_type = self._config.registry_type

        if backend_type == "memory":
            return InMemoryRegistryBackend()

        # Future implementations:
        # elif backend_type == "postgres":
        #     return PostgresRegistryBackend(**self._config.registry_config)

        raise ValueError(f"Unknown registry backend type: {backend_type}")

    def create_all(self) -> tuple[QueueBackend, LockBackend, RegistryBackend]:
        """Create all backend instances at once.

        Returns:
            A tuple of (queue_backend, lock_backend, registry_backend).
        """
        return (
            self.create_queue_backend(),
            self.create_lock_backend(),
            self.create_registry_backend(),
        )
