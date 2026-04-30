"""Task reconciler skeleton.

A lightweight local counterpart of deepwiki's task reconciler. It focuses on
repairing obviously stuck tasks in SQLite-backed local deployments.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from async_scheduler.core.models import TaskStatus
from async_scheduler.persistence import TaskRepository, get_session_no_context


class TaskReconciler:
    def __init__(self, stuck_after_seconds: int = 3600, interval_seconds: int = 300) -> None:
        self.stuck_after_seconds = stuck_after_seconds
        self.interval_seconds = interval_seconds
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._last_repaired = 0

    async def reconcile(self) -> int:
        repaired = 0
        threshold = datetime.utcnow() - timedelta(seconds=self.stuck_after_seconds)

        async with await get_session_no_context() as session:
            running = await TaskRepository.list_all(session, status=TaskStatus.RUNNING, limit=1000)
            for task in running:
                started_at = task.started_at or task.updated_at or task.created_at
                if started_at <= threshold:
                    await TaskRepository.update(
                        session,
                        task.id,
                        status=TaskStatus.FAILED,
                        error_message="reconciler: task considered stuck",
                        completed_at=datetime.utcnow(),
                    )
                    repaired += 1

        self._last_repaired = repaired
        return repaired

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        while self._running:
            await self.reconcile()
            await asyncio.sleep(self.interval_seconds)

    def is_running(self) -> bool:
        return self._running

    def stats(self) -> dict[str, int | bool]:
        return {
            "stuck_after_seconds": self.stuck_after_seconds,
            "interval_seconds": self.interval_seconds,
            "last_repaired": self._last_repaired,
            "running": self._running,
        }
