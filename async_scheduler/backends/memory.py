"""In-memory backend implementations.

These implementations provide local-only storage using Python data structures.
They are suitable for single-process deployments and serve as the default
backend for the scheduler.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from croniter import croniter

from async_scheduler.backends.base import (
    LockBackend,
    LockHandle,
    QueueBackend,
    QueueItem,
    RegistryBackend,
)
from async_scheduler.core.models import (
    Schedule,
    ScheduleCreate,
    ScheduleStatus,
    Task,
    TaskPriority,
    TaskStatus,
)
from async_scheduler.persistence import ScheduleRepository, get_session_no_context


class InMemoryQueueBackend(QueueBackend):
    """In-memory queue implementation using asyncio.PriorityQueue.

    This backend stores tasks in memory and uses asyncio's event loop
    for scheduled execution. It is not durable across restarts.
    """

    def __init__(self) -> None:
        """Initialize the in-memory queue backend."""
        self._queues: defaultdict[int, asyncio.PriorityQueue[QueueItem]] = defaultdict(
            lambda: asyncio.PriorityQueue()
        )
        self._scheduled_tasks: dict[str, asyncio.TimerHandle] = {}
        self._task_lookup: dict[str, QueueItem] = {}
        self._lock = asyncio.Lock()

    async def enqueue(
        self, task: Task, scheduled_at: datetime | None = None
    ) -> None:
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
            handle = asyncio.get_event_loop().call_later(
                delay, lambda: asyncio.create_task(_execute_scheduled())
            )
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


class InMemoryLockBackend(LockBackend):
    """In-memory lock implementation using asyncio.Lock.

    This backend provides local-only locking. For distributed scenarios,
    use a Redis-based implementation instead.
    """

    def __init__(self) -> None:
        """Initialize the in-memory lock backend."""
        self._locks: dict[str, tuple[asyncio.Lock, LockHandle]] = {}
        self._lock = asyncio.Lock()

    async def acquire(
        self,
        key: str,
        ttl: float | None = None,
        wait: float | None = None,
    ) -> LockHandle | None:
        """Acquire a lock."""
        async with self._lock:
            if key in self._locks:
                return None  # Lock already held

            inner_lock = asyncio.Lock()
            token = f"lock-{key}-{id(inner_lock)}"
            handle = LockHandle(key=key, token=token)

            # Set up TTL if specified
            if ttl:
                handle.expires_at = datetime.utcnow() + timedelta(seconds=ttl)

            self._locks[key] = (inner_lock, handle)

        # Acquire the inner lock
        if wait is None:
            acquired = await inner_lock.acquire()
        else:
            try:
                acquired = await asyncio.wait_for(inner_lock.acquire(), timeout=wait)
            except asyncio.TimeoutError:
                async with self._lock:
                    self._locks.pop(key, None)
                return None

        if acquired and ttl:
            # Auto-release after TTL
            asyncio.get_event_loop().call_later(
                ttl, lambda: asyncio.create_task(self._auto_release(key))
            )

        return handle

    async def _auto_release(self, key: str) -> None:
        """Auto-release a lock after TTL expires."""
        if key in self._locks:
            _, handle = self._locks[key]
            await self.release(handle)

    async def release(self, handle: LockHandle) -> bool:
        """Release a lock."""
        async with self._lock:
            if handle.key not in self._locks:
                return False

            inner_lock, stored_handle = self._locks[handle.key]
            if stored_handle.token != handle.token:
                return False  # Not the holder of this lock

            del self._locks[handle.key]

        inner_lock.release()
        return True

    async def extend(self, handle: LockHandle, ttl: float) -> bool:
        """Extend a lock's TTL."""
        async with self._lock:
            if handle.key not in self._locks:
                return False

            inner_lock, stored_handle = self._locks[handle.key]
            if stored_handle.token != handle.token:
                return False  # Not the holder of this lock

            # Update expiration
            stored_handle.expires_at = datetime.utcnow() + timedelta(seconds=ttl)

            # Note: In a real distributed backend, this would also
            # update the TTL on the actual lock mechanism
            return True

    async def is_locked(self, key: str) -> bool:
        """Check if a lock is currently held."""
        return key in self._locks

    async def describe_lock(self, key: str) -> dict[str, Any]:
        async with self._lock:
            entry = self._locks.get(key)
            handle = None if entry is None else entry[1]
            ttl_ms = None
            if handle is not None and handle.expires_at is not None:
                ttl_ms = max(0, int((handle.expires_at - datetime.utcnow()).total_seconds() * 1000))
            return {
                "key": key,
                "backend": "memory",
                "redis_key": None,
                "locked": handle is not None,
                "token": None if handle is None else handle.token,
                "ttl_ms": ttl_ms,
                "lease_ttl_seconds": None,
                "heartbeat_interval_seconds": None,
            }


class InMemoryRegistryBackend(RegistryBackend):
    """In-memory schedule registry implementation using SQLite persistence.

    This backend delegates to the existing ScheduleRepository for storage,
    wrapping it in the RegistryBackend interface. It maintains the same
    SQLite persistence as the original implementation.
    """

    async def create(self, schedule_create: ScheduleCreate) -> Schedule:
        """Create a new schedule."""
        async with await get_session_no_context() as session:
            return await ScheduleRepository.create(session, schedule_create)

    async def get(self, schedule_id: str) -> Schedule | None:
        """Get a schedule by ID."""
        async with await get_session_no_context() as session:
            return await ScheduleRepository.get(session, schedule_id)

    async def list_active(self, limit: int = 100) -> list[Schedule]:
        """List all active schedules."""
        async with await get_session_no_context() as session:
            return await ScheduleRepository.list_active(session, limit=limit)

    async def update(self, schedule_id: str, **kwargs) -> Schedule | None:
        """Update a schedule."""
        async with await get_session_no_context() as session:
            return await ScheduleRepository.update(session, schedule_id, **kwargs)

    async def delete(self, schedule_id: str) -> bool:
        """Delete a schedule."""
        async with await get_session_no_context() as session:
            return await ScheduleRepository.delete(session, schedule_id)

    async def list_ready(self, now: datetime | None = None) -> list[Schedule]:
        """Get schedules due for execution."""
        now = now or datetime.utcnow()
        schedules = await self.list_active(limit=1000)
        ready: list[Schedule] = []
        for schedule in schedules:
            if schedule.next_run_at is None:
                # Initialize next_run_at for new schedules
                next_fire = croniter(schedule.cron_expression, now).get_next(datetime)
                await self.advance_next_fire(
                    schedule.id, next_fire, last_triggered_at=schedule.last_run_at
                )
                continue
            if schedule.next_run_at <= now:
                ready.append(schedule)
        return ready

    async def advance_next_fire(
        self,
        schedule_id: str,
        next_fire_at: datetime,
        *,
        last_triggered_at: datetime | None,
    ) -> Schedule | None:
        """Update schedule timing after trigger."""
        async with await get_session_no_context() as session:
            return await ScheduleRepository.update(
                session,
                schedule_id,
                next_run_at=next_fire_at,
                last_run_at=last_triggered_at,
            )

    async def pause(self, schedule_id: str) -> Schedule | None:
        """Pause a schedule."""
        async with await get_session_no_context() as session:
            return await ScheduleRepository.update(
                session, schedule_id, status=ScheduleStatus.PAUSED
            )

    async def resume(self, schedule_id: str) -> Schedule | None:
        """Resume a paused schedule."""
        async with await get_session_no_context() as session:
            schedule = await ScheduleRepository.get(session, schedule_id)
            if schedule is None:
                return None
            next_fire = croniter(schedule.cron_expression, datetime.utcnow()).get_next(
                datetime
            )
            return await ScheduleRepository.update(
                session,
                schedule_id,
                status=ScheduleStatus.ACTIVE,
                next_run_at=next_fire,
            )
