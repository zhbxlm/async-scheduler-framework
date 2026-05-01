"""Abstract base classes for backend implementations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

from async_scheduler.core.models import Schedule, ScheduleCreate, Task, TaskPriority


@dataclass(order=True)
class QueueItem:
    priority: int = field(compare=True)
    created_at: datetime = field(compare=True)
    task: Task = field(compare=False)


@dataclass
class LockHandle:
    key: str
    token: str
    acquired_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime | None = None


class QueueBackend(ABC):
    @abstractmethod
    async def enqueue(self, task: Task, scheduled_at: datetime | None = None) -> None:
        pass

    @abstractmethod
    async def dequeue(self, timeout: float | None = None) -> Task | None:
        pass

    @abstractmethod
    async def peek(self, limit: int = 10) -> list[Task]:
        pass

    @abstractmethod
    async def cancel(self, task_id: str) -> bool:
        pass

    @abstractmethod
    async def update_priority(self, task_id: str, new_priority: TaskPriority) -> bool:
        pass

    @abstractmethod
    async def size(self) -> dict[int, int]:
        pass

    @abstractmethod
    async def clear(self) -> None:
        pass

    @abstractmethod
    def is_scheduled(self, task_id: str) -> bool:
        pass

    @abstractmethod
    def get_scheduled_count(self) -> int:
        pass

    @abstractmethod
    def get_queue_count(self) -> int:
        pass


class LockBackend(ABC):
    @abstractmethod
    async def acquire(
        self,
        key: str,
        ttl: float | None = None,
        wait: float | None = None,
    ) -> LockHandle | None:
        pass

    @abstractmethod
    async def release(self, handle: LockHandle) -> bool:
        pass

    @abstractmethod
    async def extend(self, handle: LockHandle, ttl: float) -> bool:
        pass

    @abstractmethod
    async def is_locked(self, key: str) -> bool:
        pass


class CompletionDedupBackend(ABC):
    """Deduplicate terminal completion processing across workers."""

    @abstractmethod
    async def claim_once(self, key: str, ttl_seconds: float | None = None) -> bool:
        """Claim a completion key for single processing.

        Returns True only for the first successful claimant.
        """
        pass

    @abstractmethod
    async def clear(self) -> None:
        pass


class RegistryBackend(ABC):
    @abstractmethod
    async def create(self, schedule_create: ScheduleCreate) -> Schedule:
        pass

    @abstractmethod
    async def get(self, schedule_id: str) -> Schedule | None:
        pass

    @abstractmethod
    async def list_active(self, limit: int = 100) -> list[Schedule]:
        pass

    @abstractmethod
    async def update(self, schedule_id: str, **kwargs) -> Schedule | None:
        pass

    @abstractmethod
    async def delete(self, schedule_id: str) -> bool:
        pass
