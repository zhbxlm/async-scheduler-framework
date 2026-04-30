"""Task reconciler.

A lightweight local counterpart of deepwiki's task reconciler. It focuses on
repairing obviously stuck tasks in SQLite-backed local deployments.

This is Batch 3 of the deepwiki distributed-alignment roadmap: strengthening
TaskReconciler integration as a platform component.

The TaskReconciler provides:
- Detection and repair of stuck tasks
- Periodic reconciliation loops
- Integration with TaskCompletionNode for cleanup
- Reconciliation metrics and tracking
- Configurable repair strategies
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable

from async_scheduler.core.models import Task, TaskStatus
from async_scheduler.persistence import TaskRepository, get_session_no_context

logger = logging.getLogger(__name__)


class RepairStrategy(str, Enum):
    """Strategies for repairing stuck tasks."""
    MARK_FAILED = "mark_failed"
    REQUEUE = "requeue"
    IGNORE = "ignore"


@dataclass
class ReconciliationMetrics:
    """Metrics for reconciliation operations."""
    total_runs: int = 0
    stuck_tasks_found: int = 0
    tasks_repaired: int = 0
    tasks_ignored: int = 0
    tasks_requeued: int = 0
    last_run_at: datetime | None = None
    last_repaired_count: int = 0

    def get_repair_rate(self) -> float:
        """Calculate the rate of repaired stuck tasks."""
        if self.stuck_tasks_found == 0:
            return 0.0
        return (self.tasks_repaired / self.stuck_tasks_found) * 100


@dataclass
class ReconciliationConfig:
    """Configuration for the task reconciler."""
    stuck_after_seconds: int = 3600
    interval_seconds: int = 300
    max_tasks_per_run: int = 1000
    repair_strategy: RepairStrategy = RepairStrategy.MARK_FAILED
    check_timeouts: bool = True
    check_orphaned: bool = True


class TaskReconciler:
    """Platform component for reconciling stuck tasks.

    The TaskReconciler is responsible for:
    - Detecting tasks that have been running too long (stuck tasks)
    - Repairing stuck tasks according to configured strategies
    - Periodic background reconciliation
    - Tracking reconciliation metrics
    - Integration with other platform components

    This component is a key part of the deepwiki architecture, providing
    self-healing capabilities for the scheduler.

    Args:
        config: Optional ReconciliationConfig instance.
    """

    def __init__(
        self,
        config: ReconciliationConfig | None = None,
        completion_node: Any | None = None,
    ) -> None:
        """Initialize the task reconciler."""
        self.config = config or ReconciliationConfig()
        self.completion_node = completion_node
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._metrics = ReconciliationMetrics()
        self._reconciliation_handlers: list[Callable[[Task], None]] = []

    async def reconcile(self) -> int:
        """Run a single reconciliation pass.

        This method:
        1. Finds tasks that have been running too long
        2. Applies the configured repair strategy
        3. Updates reconciliation metrics
        4. Calls registered reconciliation handlers

        Returns:
            Number of tasks repaired in this pass.
        """
        self._metrics.total_runs += 1
        self._metrics.last_run_at = datetime.utcnow()
        repaired = 0
        stuck_tasks: list[Task] = []

        # Find stuck tasks
        threshold = datetime.utcnow() - timedelta(seconds=self.config.stuck_after_seconds)

        async with await get_session_no_context() as session:
            # Check running tasks
            if self.config.check_timeouts:
                running = await TaskRepository.list_all(
                    session, status=TaskStatus.RUNNING, limit=self.config.max_tasks_per_run
                )
                for task in running:
                    started_at = task.started_at or task.updated_at or task.created_at
                    if started_at <= threshold:
                        stuck_tasks.append(task)

            # Check for orphaned tasks (queued but not running for too long)
            if self.config.check_orphaned:
                queued = await TaskRepository.list_all(
                    session, status=TaskStatus.QUEUED, limit=self.config.max_tasks_per_run
                )
                for task in queued:
                    updated_at = task.updated_at or task.created_at
                    if updated_at <= threshold:
                        stuck_tasks.append(task)

            self._metrics.stuck_tasks_found += len(stuck_tasks)

            # Repair stuck tasks
            for task in stuck_tasks:
                if repaired >= self.config.max_tasks_per_run:
                    break

                result = await self._repair_task(session, task)
                if result:
                    repaired += 1

        self._metrics.last_repaired_count = repaired

        # Call reconciliation handlers
        for handler in self._reconciliation_handlers:
            try:
                handler_metrics = {"repaired": repaired, "stuck_found": len(stuck_tasks)}
                handler(handler_metrics)
            except Exception:
                logger.exception("Reconciliation handler failed")

        logger.info(
            f"Reconciliation pass completed: found {len(stuck_tasks)} stuck tasks, repaired {repaired}"
        )

        return repaired

    async def _repair_task(self, session, task: Task) -> bool:
        """Repair a single stuck task.

        Args:
            session: Database session.
            task: The task to repair.

        Returns:
            True if repaired, False otherwise.
        """
        error_message = "reconciler: task considered stuck after " f"{self.config.stuck_after_seconds} seconds"

        if self.config.repair_strategy == RepairStrategy.IGNORE:
            self._metrics.tasks_ignored += 1
            return False

        if self.config.repair_strategy == RepairStrategy.MARK_FAILED:
            await TaskRepository.update(
                session,
                task.id,
                status=TaskStatus.FAILED,
                error_message=error_message,
                completed_at=datetime.utcnow(),
            )
            self._metrics.tasks_repaired += 1
            return True

        if self.config.repair_strategy == RepairStrategy.REQUEUE:
            # Reset status and allow re-queuing
            await TaskRepository.update(
                session,
                task.id,
                status=TaskStatus.PENDING,
                error_message=error_message,
                started_at=None,
                retry_count=task.retry_count + 1,
            )
            self._metrics.tasks_requeued += 1
            return True

        return False

    async def start(self) -> None:
        """Start the background reconciliation loop."""
        if self._running:
            logger.warning("Reconciler is already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("Task reconciler started")

    async def stop(self) -> None:
        """Stop the background reconciliation loop."""
        if not self._running:
            return

        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("Task reconciler stopped")

    async def _loop(self) -> None:
        """Main reconciliation loop."""
        while self._running:
            try:
                await self.reconcile()
                await asyncio.sleep(self.config.interval_seconds)
            except asyncio.CancelledError:
                logger.info("Reconciliation loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in reconciliation loop: {e}", exc_info=True)
                await asyncio.sleep(self.config.interval_seconds)

    def register_handler(self, handler: Callable[[dict[str, Any]], None]) -> None:
        """Register a reconciliation handler.

        Handlers are called after each reconciliation pass with metrics.

        Args:
            handler: The handler function.
        """
        self._reconciliation_handlers.append(handler)

    def unregister_handler(self, handler: Callable[[dict[str, Any]], None]) -> bool:
        """Unregister a reconciliation handler.

        Args:
            handler: The handler function to remove.

        Returns:
            True if removed, False if not found.
        """
        try:
            self._reconciliation_handlers.remove(handler)
            return True
        except ValueError:
            return False

    def is_running(self) -> bool:
        """Check if the reconciler is running."""
        return self._running

    def get_metrics(self) -> ReconciliationMetrics:
        """Get the current reconciliation metrics.

        Returns:
            The ReconciliationMetrics.
        """
        return self._metrics

    def reset_metrics(self) -> None:
        """Reset all reconciliation metrics."""
        self._metrics = ReconciliationMetrics()

    def update_config(self, **kwargs: Any) -> None:
        """Update the reconciler configuration.

        Args:
            **kwargs: Configuration parameters to update.
        """
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
