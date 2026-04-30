"""Abstract base classes for backend implementations.

These abstractions define the contracts that all backend implementations must follow,
enabling pluggable storage backends for the distributed scheduler.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from async_scheduler.core.models import Schedule, ScheduleCreate, Task, TaskPriority


@dataclass(order=True)
class QueueItem:
    """A single item in the priority queue."""

    priority: int = field(compare=True)
    created_at: datetime = field(compare=True)
    task: Task = field(compare=False)


@dataclass
class LockHandle:
    """Handle for an acquired lock."""

    key: str
    token: str
    acquired_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime | None = None


class QueueBackend(ABC):
    """Abstract base class for queue backends.

    A queue backend manages task prioritization and delayed execution.
    Implementations can be in-memory (for local deployment) or distributed
    (e.g., Redis, RabbitMQ for distributed systems).
    """

    @abstractmethod
    async def enqueue(
        self, task: Task, scheduled_at: datetime | None = None
    ) -> None:
        """Add a task to the appropriate queue.

        Args:
            task: The task to enqueue.
            scheduled_at: If provided and in the future, schedule the task for
                later execution instead of immediate queuing.
        """
        pass

    @abstractmethod
    async def dequeue(self, timeout: float | None = None) -> Task | None:
        """Get the next highest priority task.

        Args:
            timeout: Maximum time to wait for a task. None means wait indefinitely.

        Returns:
            The next task, or None if timeout expires.
        """
        pass

    @abstractmethod
    async def peek(self, limit: int = 10) -> list[Task]:
        """Peek at the next tasks without removing them.

        Args:
            limit: Maximum number of tasks to return.

        Returns:
            List of upcoming tasks.
        """
        pass

    @abstractmethod
    async def cancel(self, task_id: str) -> bool:
        """Cancel a scheduled or queued task.

        Args:
            task_id: ID of the task to cancel.

        Returns:
            True if the task was cancelled, False if not found.
        """
        pass

    @abstractmethod
    async def update_priority(self, task_id: str, new_priority: TaskPriority) -> bool:
        """Update a task's priority in the queue.

        Args:
            task_id: ID of the task to update.
            new_priority: New priority level.

        Returns:
            True if updated, False if task not found.
        """
        pass

    @abstractmethod
    async def size(self) -> dict[int, int]:
        """Get the size of each priority queue.

        Returns:
            Dict mapping priority levels to queue sizes.
        """
        pass

    @abstractmethod
    async def clear(self) -> None:
        """Clear all queues and scheduled tasks."""
        pass

    @abstractmethod
    def is_scheduled(self, task_id: str) -> bool:
        """Check if a task is scheduled for future execution.

        Args:
            task_id: ID of the task to check.

        Returns:
            True if the task is scheduled, False otherwise.
        """
        pass

    @abstractmethod
    def get_scheduled_count(self) -> int:
        """Get the count of scheduled tasks.

        Returns:
            Number of tasks scheduled for future execution.
        """
        pass

    @abstractmethod
    def get_queue_count(self) -> int:
        """Get the total count of tasks in queues.

        Returns:
            Total number of tasks in immediate queues.
        """
        pass


class LockBackend(ABC):
    """Abstract base class for distributed lock backends.

    Lock backends provide distributed locking capabilities for coordinating
    access to shared resources across multiple scheduler instances.
    """

    @abstractmethod
    async def acquire(
        self,
        key: str,
        ttl: float | None = None,
        wait: float | None = None,
    ) -> LockHandle | None:
        """Acquire a lock.

        Args:
            key: Unique lock identifier.
            ttl: Time-to-live in seconds. If None, lock never auto-expires.
            wait: Maximum time to wait for acquisition. If None, fail immediately.

        Returns:
            LockHandle if acquired, None if lock unavailable.
        """
        pass

    @abstractmethod
    async def release(self, handle: LockHandle) -> bool:
        """Release a lock.

        Args:
            handle: The lock handle from acquire().

        Returns:
            True if released, False if handle invalid or expired.
        """
        pass

    @abstractmethod
    async def extend(self, handle: LockHandle, ttl: float) -> bool:
        """Extend a lock's TTL.

        Args:
            handle: The lock handle from acquire().
            ttl: New TTL in seconds.

        Returns:
            True if extended, False if handle invalid or expired.
        """
        pass

    @abstractmethod
    async def is_locked(self, key: str) -> bool:
        """Check if a lock is currently held.

        Args:
            key: Unique lock identifier.

        Returns:
            True if locked, False otherwise.
        """
        pass


class RegistryBackend(ABC):
    """Abstract base class for schedule registry backends.

    Registry backends manage the storage and retrieval of schedule definitions
    and their execution state. Implementations can use databases, distributed
    stores, or other persistence mechanisms.
    """

    @abstractmethod
    async def create(self, schedule_create: ScheduleCreate) -> Schedule:
        """Create a new schedule.

        Args:
            schedule_create: Schedule creation data.

        Returns:
            The created schedule.
        """
        pass

    @abstractmethod
    async def get(self, schedule_id: str) -> Schedule | None:
        """Get a schedule by ID.

        Args:
            schedule_id: ID of the schedule.

        Returns:
            The schedule, or None if not found.
        """
        pass

    @abstractmethod
    async def list_active(self, limit: int = 100) -> list[Schedule]:
        """List all active schedules.

        Args:
            limit: Maximum number of schedules to return.

        Returns:
            List of active schedules.
        """
        pass

    @abstractmethod
    async def list_ready(self, now: datetime | None = None) -> list[Schedule]:
        """Get schedules due for execution.

        Args:
            now: Current time for comparison. Defaults to current UTC time.

        Returns:
            List of schedules ready to fire.
        """
        pass

    @abstractmethod
    async def advance_next_fire(
        self,
        schedule_id: str,
        next_fire_at: datetime,
        *,
        last_triggered_at: datetime | None,
    ) -> Schedule | None:
        """Update schedule timing after trigger.

        Args:
            schedule_id: ID of the schedule.
            next_fire_at: Next scheduled execution time.
            last_triggered_at: Last execution time.

        Returns:
            Updated schedule, or None if not found.
        """
        pass

    @abstractmethod
    async def pause(self, schedule_id: str) -> Schedule | None:
        """Pause a schedule.

        Args:
            schedule_id: ID of the schedule.

        Returns:
            Updated schedule, or None if not found.
        """
        pass

    @abstractmethod
    async def resume(self, schedule_id: str) -> Schedule | None:
        """Resume a paused schedule.

        Args:
            schedule_id: ID of the schedule.

        Returns:
            Updated schedule, or None if not found.
        """
        pass
