"""Schedule registry abstraction.

This is a local/SQLite-backed counterpart of the deepwiki ScheduleRegistry,
introduced to keep lifecycle logic separate from CronScheduler.

This module now uses pluggable backends (RegistryBackend) to enable
distributed scheduler support while maintaining backward compatibility
with the existing SQLite-based implementation.

This is part of the deepwiki distributed-alignment roadmap (Batch 1).

Batch 2 enhancements:
- Added delete() operation for schedule removal
- Added update() operation for schedule modification
- Added batch operations for bulk lifecycle management
- Enhanced registry with status filtering and query capabilities
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from async_scheduler.backends import InMemoryRegistryBackend, RegistryBackend
from async_scheduler.core.models import Schedule, ScheduleCreate, ScheduleStatus


class ScheduleRegistry:
    """Registry facade for schedule lifecycle operations using pluggable backends.

    The ScheduleRegistry provides a unified interface for schedule operations
    while delegating storage and retrieval to a RegistryBackend. By default,
    it uses an InMemoryRegistryBackend (which internally uses SQLite).

    This is Batch 2 of the deepwiki distributed-alignment roadmap: expanding
    lifecycle control and moving more schedule operations behind the registry.

    Args:
        backend: RegistryBackend instance. If None, creates an InMemoryRegistryBackend.
    """

    def __init__(self, backend: RegistryBackend | None = None) -> None:
        """Initialize the registry with a backend."""
        self._backend: RegistryBackend = backend or InMemoryRegistryBackend()

    # Basic CRUD operations

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

    async def list_by_status(self, status: ScheduleStatus, limit: int = 100) -> list[Schedule]:
        """List schedules by status.

        Args:
            status: The schedule status to filter by.
            limit: Maximum number of schedules to return.

        Returns:
            List of schedules with the specified status.
        """
        schedules = await self._backend.list_active(limit=limit * 2)
        return [s for s in schedules if s.status == status][:limit]

    async def list_ready(self, now: datetime | None = None) -> list[Schedule]:
        """Get schedules due for execution.

        Delegates to the underlying backend.

        Args:
            now: Current time for comparison. Defaults to current UTC time.

        Returns:
            List of schedules ready to fire.
        """
        return await self._backend.list_ready(now)

    # Lifecycle control operations

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

    async def delete(self, schedule_id: str) -> bool:
        """Delete a schedule.

        Args:
            schedule_id: ID of the schedule to delete.

        Returns:
            True if deleted, False if not found.
        """
        from async_scheduler.persistence import ScheduleRepository, get_session_no_context

        async with await get_session_no_context() as session:
            deleted = await ScheduleRepository.delete(session, schedule_id)
            return deleted is not None and deleted

    async def update(
        self,
        schedule_id: str,
        **updates: Any,
    ) -> Schedule | None:
        """Update a schedule's attributes.

        Args:
            schedule_id: ID of the schedule to update.
            **updates: Fields to update (e.g., cron_expression, task_template, etc.).

        Returns:
            Updated schedule, or None if not found.
        """
        from async_scheduler.persistence import ScheduleRepository, get_session_no_context

        async with await get_session_no_context() as session:
            return await ScheduleRepository.update(session, schedule_id, **updates)

    # Batch operations

    async def pause_all(self, tenant_id: str | None = None) -> int:
        """Pause all active schedules, optionally scoped to a tenant.

        Args:
            tenant_id: Optional tenant ID to filter schedules.

        Returns:
            Number of schedules paused.
        """
        schedules = await self.list_active(limit=1000)
        if tenant_id:
            schedules = [s for s in schedules if s.tenant_id == tenant_id]

        paused = 0
        for schedule in schedules:
            if await self.pause(schedule.id):
                paused += 1
        return paused

    async def resume_all(self, tenant_id: str | None = None) -> int:
        """Resume all paused schedules, optionally scoped to a tenant.

        Args:
            tenant_id: Optional tenant ID to filter schedules.

        Returns:
            Number of schedules resumed.
        """
        paused_schedules = await self.list_by_status(ScheduleStatus.PAUSED, limit=1000)
        if tenant_id:
            paused_schedules = [s for s in paused_schedules if s.tenant_id == tenant_id]

        resumed = 0
        for schedule in paused_schedules:
            if await self.resume(schedule.id):
                resumed += 1
        return resumed

    async def delete_all(self, tenant_id: str | None = None, status: ScheduleStatus | None = None) -> int:
        """Delete multiple schedules by criteria.

        Args:
            tenant_id: Optional tenant ID to filter schedules.
            status: Optional status to filter schedules.

        Returns:
            Number of schedules deleted.
        """
        schedules = await self._backend.list_active(limit=1000)

        if tenant_id:
            schedules = [s for s in schedules if s.tenant_id == tenant_id]
        if status:
            schedules = [s for s in schedules if s.status == status]

        deleted = 0
        for schedule in schedules:
            if await self.delete(schedule.id):
                deleted += 1
        return deleted

    # Query and inspection

    async def get_count(self, status: ScheduleStatus | None = None) -> int:
        """Get the count of schedules, optionally filtered by status.

        Args:
            status: Optional status to filter by.

        Returns:
            Number of schedules matching the criteria.
        """
        if status:
            return len(await self.list_by_status(status, limit=10000))
        return len(await self.list_active(limit=10000))

    async def exists(self, schedule_id: str) -> bool:
        """Check if a schedule exists.

        Args:
            schedule_id: ID of the schedule to check.

        Returns:
            True if the schedule exists, False otherwise.
        """
        return await self.get(schedule_id) is not None

    async def get_by_name(self, name: str, tenant_id: str | None = None) -> Schedule | None:
        """Get a schedule by name (and optionally tenant).

        Args:
            name: Name of the schedule.
            tenant_id: Optional tenant ID to scope the search.

        Returns:
            The schedule, or None if not found.
        """
        schedules = await self.list_active(limit=1000)
        if tenant_id:
            schedules = [s for s in schedules if s.tenant_id == tenant_id]

        for schedule in schedules:
            if schedule.name == name:
                return schedule
        return None

    @property
    def backend(self) -> RegistryBackend:
        """Get the underlying backend instance.

        This property allows direct access to the backend for advanced use cases.

        Returns:
            The RegistryBackend instance.
        """
        return self._backend
