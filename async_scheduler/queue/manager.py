"""Queue manager for task prioritization and scheduling."""

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from async_scheduler.core.models import Task, TaskPriority, TaskStatus


@dataclass(order=True)
class QueueItem:
    """A single item in the priority queue."""

    priority: int = field(compare=True)
    created_at: datetime = field(compare=True)
    task: Task = field(compare=False)


class QueueManager:
    """Manages task queues with priority support."""

    def __init__(self) -> None:
        """Initialize the queue manager."""
        self._queues: defaultdict[int, asyncio.PriorityQueue[QueueItem]] = defaultdict(
            lambda: asyncio.PriorityQueue()
        )
        self._scheduled_tasks: dict[str, asyncio.TimerHandle] = {}
        self._task_lookup: dict[str, QueueItem] = {}
        self._lock = asyncio.Lock()

    async def enqueue(self, task: Task, scheduled_at: datetime | None = None) -> None:
        """Add a task to the appropriate queue."""
        async with self._lock:
            if scheduled_at and scheduled_at > datetime.utcnow():
                # Schedule for later execution
                self._schedule_task(task, scheduled_at)
            else:
                # Add to immediate queue based on priority
                item = QueueItem(
                    priority=task.priority.value,
                    created_at=task.created_at,
                    task=task,
                )
                await self._queues[task.priority.value].put(item)
                self._task_lookup[task.id] = item

    def _schedule_task(self, task: Task, scheduled_at: datetime) -> None:
        """Schedule a task for future execution."""

        async def _execute_scheduled() -> None:
            try:
                # Update scheduled_at and enqueue
                await self.enqueue(task, None)
            except Exception:
                pass  # Task might have been cancelled

        delay = (scheduled_at - datetime.utcnow()).total_seconds()
        if delay > 0:
            handle = asyncio.get_event_loop().call_later(delay, lambda: asyncio.create_task(_execute_scheduled()))
            self._scheduled_tasks[task.id] = handle

    async def dequeue(self, timeout: float | None = None) -> Task | None:
        """Get the next highest priority task."""
        # Check queues in priority order (highest first)
        for priority in sorted(self._queues.keys(), reverse=True):
            queue = self._queues[priority]
            try:
                item = await asyncio.wait_for(queue.get(), timeout=timeout)
                self._task_lookup.pop(item.task.id, None)
                return item.task
            except asyncio.TimeoutError:
                continue
        return None

    async def peek(self, limit: int = 10) -> list[Task]:
        """Peek at the next tasks without removing them."""
        tasks: list[Task] = []

        for priority in sorted(self._queues.keys(), reverse=True):
            queue = self._queues[priority]
            size = queue.qsize()

            # Create a temporary queue for peeking
            temp_items: list[QueueItem] = []

            for _ in range(min(size, limit - len(tasks))):
                item = queue.get_nowait()
                tasks.append(item.task)
                temp_items.append(item)

            # Put items back
            for item in temp_items:
                queue.put_nowait(item)

            if len(tasks) >= limit:
                break

        return tasks

    async def cancel(self, task_id: str) -> bool:
        """Cancel a scheduled or queued task."""
        async with self._lock:
            # Cancel scheduled task
            if task_id in self._scheduled_tasks:
                handle = self._scheduled_tasks.pop(task_id)
                handle.cancel()
                return True

            # Remove from queue
            if task_id in self._task_lookup:
                item = self._task_lookup.pop(task_id)
                # Mark as cancelled in the item's task
                item.task.status = TaskStatus.CANCELLED
                # We can't easily remove from PriorityQueue, so we'll skip it when dequeuing
                return True

            return False

    async def update_priority(self, task_id: str, new_priority: TaskPriority) -> bool:
        """Update a task's priority in the queue."""
        async with self._lock:
            if task_id not in self._task_lookup:
                return False

            old_item = self._task_lookup.pop(task_id)
            old_item.task.priority = new_priority

            new_item = QueueItem(
                priority=new_priority.value,
                created_at=old_item.created_at,
                task=old_item.task,
            )

            await self._queues[new_priority.value].put(new_item)
            self._task_lookup[task_id] = new_item

            return True

    async def size(self) -> dict[int, int]:
        """Get the size of each priority queue."""
        sizes: dict[int, int] = {}
        for priority, queue in self._queues.items():
            sizes[priority] = queue.qsize()
        return sizes

    async def clear(self) -> None:
        """Clear all queues."""
        async with self._lock:
            for handle in self._scheduled_tasks.values():
                handle.cancel()
            self._scheduled_tasks.clear()
            self._task_lookup.clear()

            for queue in self._queues.values():
                while not queue.empty():
                    queue.get_nowait()

    def is_scheduled(self, task_id: str) -> bool:
        """Check if a task is scheduled for future execution."""
        return task_id in self._scheduled_tasks

    def get_scheduled_count(self) -> int:
        """Get the count of scheduled tasks."""
        return len(self._scheduled_tasks)

    def get_queue_count(self) -> int:
        """Get the total count of tasks in queues."""
        return sum(queue.qsize() for queue in self._queues.values())
