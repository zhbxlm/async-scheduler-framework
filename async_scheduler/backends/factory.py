"""Factory for creating backend instances.

The factory pattern allows for easy configuration and instantiation of
different backend implementations without requiring changes to the
core scheduler code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator

from async_scheduler.backends.base import LockBackend, QueueBackend, RegistryBackend
from async_scheduler.backends.memory import (
    InMemoryLockBackend,
    InMemoryQueueBackend,
    InMemoryRegistryBackend,
)
from async_scheduler.backends.redis import RedisQueueBackend


class BackendConfig(BaseModel):
    """Configuration for backend selection and initialization.

    This configuration allows users to select different backend implementations
    without modifying the scheduler code. Future versions will support additional
    backend types like "redis", "postgresql", etc.

    Attributes:
        queue_type: Type of queue backend ("memory" for in-memory, "redis" for Redis, etc.)
        lock_type: Type of lock backend ("memory" for in-memory, "redis" for Redis, etc.)
        registry_type: Type of registry backend ("memory" for in-memory/SQLite, etc.)
        redis_url: Shared Redis connection URL for distributed-mode scaffolding.
        lease_ttl_seconds: Default task lease TTL for distributed execution.
        heartbeat_interval_seconds: Worker heartbeat interval in distributed mode.
        queue_config: Additional configuration for queue backend.
        lock_config: Additional configuration for lock backend.
        registry_config: Additional configuration for registry backend.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    queue_type: str = "memory"
    lock_type: str = "memory"
    registry_type: str = "memory"
    redis_url: str | None = None
    lease_ttl_seconds: float = 30.0
    heartbeat_interval_seconds: float = 10.0
    queue_config: dict[str, Any] = field(default_factory=dict)
    lock_config: dict[str, Any] = field(default_factory=dict)
    registry_config: dict[str, Any] = field(default_factory=dict)

    @property
    def distributed_mode(self) -> bool:
        return any(backend_type == "redis" for backend_type in (self.queue_type, self.lock_type))

    @model_validator(mode="after")
    def validate_distributed_settings(self) -> "BackendConfig":
        if self.distributed_mode and not self.redis_url:
            raise ValueError("redis_url is required when using Redis backends")

        self.queue_config = self._merge_redis_defaults(self.queue_config)
        self.lock_config = self._merge_redis_defaults(self.lock_config)
        return self

    def _merge_redis_defaults(self, config: dict[str, Any]) -> dict[str, Any]:
        merged = dict(config)
        if self.redis_url:
            merged.setdefault("redis_url", self.redis_url)
        merged.setdefault("lease_ttl_seconds", self.lease_ttl_seconds)
        merged.setdefault("heartbeat_interval_seconds", self.heartbeat_interval_seconds)
        return merged

    @classmethod
    def default(cls) -> "BackendConfig":
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

        if backend_type == "redis":
            return RedisQueueBackend(**self._config.queue_config)

        # Future implementations:
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

        if backend_type == "redis":
            from async_scheduler.backends.redis import RedisLockBackend
            return RedisLockBackend(**self._config.lock_config)

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
