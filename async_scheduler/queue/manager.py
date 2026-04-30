"""Queue manager for task prioritization and scheduling.

The QueueManager provides a facade over pluggable backend implementations,
allowing the scheduler to use different storage mechanisms (in-memory, Redis, etc.)
without changing the application code.

This is part of the deepwiki distributed-alignment roadmap (Batch 1).
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from async_scheduler.backends import InMemoryQueueBackend, QueueBackend
from async_scheduler.core.models import Task, TaskPriority

if TYPE_CHECKING:
    from async_scheduler.backends.base import QueueItem

__all__ = ["QueueManager", "QueueItem"]


# Re-export QueueItem from backends for backward compatibility
from async_scheduler.backends.base import QueueItem  # noqa: E402


class QueueManager:
    """Manages task queues with priority support using pluggable backends.

    The QueueManager provides a unified interface for task queuing operations
    while delegating the actual storage and execution to a QueueBackend.
    By default, it uses an in-memory backend for local deployment.

    Args:
        backend: QueueBackend instance. If None, creates an InMemoryQueueBackend.
    """

    def __init__(self, backend: QueueBackend | None = None) -> None:
        """Initialize the queue manager with a backend."""
        self._backend: QueueBackend = backend or InMemoryQueueBackend()


    async def enqueue(self, task: Task, scheduled_at: datetime | None = None) -> None:
        """Add a task to the appropriate queue.

        Delegates to the underlying backend.

        Args:
            task: The task to enqueue.
            scheduled_at: Optional future execution time.
        """
        await self._backend.enqueue(task, scheduled_at)

    async def dequeue(self, timeout: float | None = None) -> Task | None:
        """Get the next highest priority task.

        Delegates to the underlying backend.

        Args:
            timeout: Maximum time to wait for a task.

        Returns:
            The next task, or None if timeout expires.
        """
        return await self._backend.dequeue(timeout)

    async def peek(self, limit: int = 10) -> list[Task]:
        """Peek at the next tasks without removing them.

        Delegates to the underlying backend.

        Args:
            limit: Maximum number of tasks to return.

        Returns:
            List of upcoming tasks.
        """
        return await self._backend.peek(limit)

    async def cancel(self, task_id: str) -> bool:
        """Cancel a scheduled or queued task.

        Delegates to the underlying backend.

        Args:
            task_id: ID of the task to cancel.

        Returns:
            True if cancelled, False if not found.
        """
        return await self._backend.cancel(task_id)

    async def update_priority(self, task_id: str, new_priority: TaskPriority) -> bool:
        """Update a task's priority in the queue.

        Delegates to the underlying backend.

        Args:
            task_id: ID of the task to update.
            new_priority: New priority level.

        Returns:
            True if updated, False if not found.
        """
        return await self._backend.update_priority(task_id, new_priority)

    async def size(self) -> dict[int, int]:
        """Get the size of each priority queue.

        Delegates to the underlying backend.

        Returns:
            Dict mapping priority levels to queue sizes.
        """
        return await self._backend.size()

    async def clear(self) -> None:
        """Clear all queues.

        Delegates to the underlying backend.
        """
        await self._backend.clear()

    def is_scheduled(self, task_id: str) -> bool:
        """Check if a task is scheduled for future execution.

        Delegates to the underlying backend.

        Args:
            task_id: ID of the task to check.

        Returns:
            True if the task is scheduled, False otherwise.
        """
        return self._backend.is_scheduled(task_id)

    def get_scheduled_count(self) -> int:
        """Get the count of scheduled tasks.

        Delegates to the underlying backend.

        Returns:
            Number of tasks scheduled for future execution.
        """
        return self._backend.get_scheduled_count()

    def get_queue_count(self) -> int:
        """Get the total count of tasks in queues.

        Delegates to the underlying backend.

        Returns:
            Total number of tasks in immediate queues.
        """
        return self._backend.get_queue_count()

    @property
    def backend(self) -> QueueBackend:
        """Get the underlying backend instance.

        This property allows direct access to the backend for advanced use cases.

        Returns:
            The QueueBackend instance.
        """
        return self._backend

