"""Schedule registry abstraction.

This is a local/SQLite-backed counterpart of the deepwiki ScheduleRegistry,
introduced to keep lifecycle logic separate from CronScheduler.

This module now uses pluggable backends (RegistryBackend) to enable
distributed scheduler support while maintaining backward compatibility
with the existing SQLite-based implementation.

This is part of the deepwiki distributed-alignment roadmap (Batch 1).
"""

from __future__ import annotations

from datetime import datetime

from async_scheduler.backends import InMemoryRegistryBackend, RegistryBackend
from async_scheduler.core.models import Schedule, ScheduleCreate


class ScheduleRegistry:
    """Registry facade for schedule lifecycle operations using pluggable backends.

    The ScheduleRegistry provides a unified interface for schedule operations
    while delegating storage and retrieval to a RegistryBackend. By default,
    it uses an InMemoryRegistryBackend (which internally uses SQLite).

    Args:
        backend: RegistryBackend instance. If None, creates an InMemoryRegistryBackend.
    """

    def __init__(self, backend: RegistryBackend | None = None) -> None:
        """Initialize the registry with a backend."""
        self._backend: RegistryBackend = backend or InMemoryRegistryBackend()


    async def create(self, schedule_create: ScheduleCreate) -> Schedule:
        """Create a new schedule.

        Delegates to the underlying backend.

        Args:
            schedule_create: Schedule creation data.

        Returns:
            The created schedule.
        """
        return await self._backend.create(schedule_create)

    async def get(self, schedule_id: str) -> Schedule | None:
        """Get a schedule by ID.

        Delegates to the underlying backend.

        Args:
            schedule_id: ID of the schedule.

        Returns:
            The schedule, or None if not found.
        """
        return await self._backend.get(schedule_id)

    async def list_active(self, limit: int = 100) -> list[Schedule]:
        """List all active schedules.

        Delegates to the underlying backend.

        Args:
            limit: Maximum number of schedules to return.

        Returns:
            List of active schedules.
        """
        return await self._backend.list_active(limit)

    async def list_ready(self, now: datetime | None = None) -> list[Schedule]:
        """Get schedules due for execution.

        Delegates to the underlying backend.

        Args:
            now: Current time for comparison. Defaults to current UTC time.

        Returns:
            List of schedules ready to fire.
        """
        return await self._backend.list_ready(now)

    async def advance_next_fire(
        self,
        schedule_id: str,
        next_fire_at: datetime,
        *,
        last_triggered_at: datetime | None,
    ) -> Schedule | None:
        """Update schedule timing after trigger.

        Delegates to the underlying backend.

        Args:
            schedule_id: ID of the schedule.
            next_fire_at: Next scheduled execution time.
            last_triggered_at: Last execution time.

        Returns:
            Updated schedule, or None if not found.
        """
        return await self._backend.advance_next_fire(
            schedule_id, next_fire_at, last_triggered_at=last_triggered_at
        )

    async def pause(self, schedule_id: str) -> Schedule | None:
        """Pause a schedule.

        Delegates to the underlying backend.

        Args:
            schedule_id: ID of the schedule.

        Returns:
            Updated schedule, or None if not found.
        """
        return await self._backend.pause(schedule_id)

    async def resume(self, schedule_id: str) -> Schedule | None:
        """Resume a paused schedule.

        Delegates to the underlying backend.

        Args:
            schedule_id: ID of the schedule.

        Returns:
            Updated schedule, or None if not found.
        """
        return await self._backend.resume(schedule_id)

    @property
    def backend(self) -> RegistryBackend:
        """Get the underlying backend instance.

        This property allows direct access to the backend for advanced use cases.

        Returns:
            The RegistryBackend instance.
        """
        return self._backend
