"""Backend abstraction layer for distributed scheduler support.

This module provides abstract base classes for queue, lock, and registry backends,
enabling the scheduler to operate with different storage implementations
(in-memory, Redis, etc.) without changing core business logic.

This is part of the deepwiki distributed-alignment roadmap (Batch 1).
"""

from async_scheduler.backends.base import (
    LockBackend,
    LockHandle,
    QueueBackend,
    QueueItem,
    RegistryBackend,
)
from async_scheduler.backends.factory import (
    BackendConfig,
    BackendFactory,
)
from async_scheduler.backends.memory import (
    InMemoryLockBackend,
    InMemoryQueueBackend,
    InMemoryRegistryBackend,
)
from async_scheduler.backends.redis import RedisLockBackend, RedisQueueBackend

__all__ = [
    # Abstract base classes
    "QueueBackend",
    "LockBackend",
    "RegistryBackend",
    # Data classes
    "QueueItem",
    "LockHandle",
    # In-memory implementations
    "InMemoryQueueBackend",
    "InMemoryLockBackend",
    "InMemoryRegistryBackend",
    "RedisQueueBackend",
    "RedisLockBackend",
    # Factory
    "BackendFactory",
    "BackendConfig",
]