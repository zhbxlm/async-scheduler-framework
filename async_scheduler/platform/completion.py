"""Task completion node.

The TaskCompletionNode separates final-state persistence and callback dispatch
from the consumer loop, closer to the deepwiki task completion node concept.

This is Batch 3 of the deepwiki distributed-alignment roadmap: strengthening
TaskCompletionNode and TaskReconciler integration as platform components.

The TaskCompletionNode provides:
- Final task state persistence
- Callback dispatch on completion
- Completion tracking and metrics
- Integration with the platform service container
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable

from async_scheduler.core.models import Task, TaskStatus
from async_scheduler.persistence import TaskRepository, get_session_no_context
from async_scheduler.platform.callback import CallbackDispatcher

logger = logging.getLogger(__name__)


@dataclass
class CompletionMetrics:
    """Metrics for task completions."""
    total_completed: int = 0
    successful_completions: int = 0
    failed_completions: int = 0
    cancelled_completions: int = 0
    timeout_completions: int = 0
    callback_dispatches: int = 0
    callback_failures: int = 0

    def get_success_rate(self) -> float:
        """Calculate completion success rate."""
        if self.total_completed == 0:
            return 0.0
        return (self.successful_completions / self.total_completed) * 100

    def get_callback_success_rate(self) -> float:
        """Calculate callback dispatch success rate."""
        if self.callback_dispatches == 0:
            return 0.0
        return ((self.callback_dispatches - self.callback_failures) / self.callback_dispatches) * 100


from dataclasses import dataclass


class TaskCompletionNode:
    """Platform component for finalizing task execution.

    The TaskCompletionNode is responsible for:
    - Persisting final task state to the database
    - Dispatching callbacks on task completion
    - Tracking completion metrics
    - Handling idempotency for completion operations

    This component is a key part of the deepwiki architecture, separating
    completion concerns from the consumer loop.

    Args:
        callback_dispatcher: Optional CallbackDispatcher instance.
        enable_metrics: Whether to track completion metrics (default: True).
    """

    def __init__(
        self,
        callback_dispatcher: CallbackDispatcher | None = None,
        enable_metrics: bool = True,
    ) -> None:
        """Initialize the task completion node."""
        self.callback_dispatcher = callback_dispatcher or CallbackDispatcher()
        self._metrics = CompletionMetrics() if enable_metrics else None
        self._enable_metrics = enable_metrics
        self._completion_handlers: dict[TaskStatus, list[Callable[[Task], None]]] = defaultdict(list)

    async def finalize(
        self,
        task: Task,
        status: TaskStatus,
        *,
        error_message: str | None = None,
        result: dict[str, Any] | None = None,
    ) -> Task | None:
        """Finalize a task with its final status.

        This method:
        1. Calculates timing information (started_at, completed_at)
        2. Persists the final state to the database
        3. Dispatches callbacks if applicable
        4. Updates completion metrics
        5. Calls registered completion handlers

        Args:
            task: The task to finalize.
            status: The final status of the task.
            error_message: Optional error message if task failed.
            result: Optional result data if task succeeded.

        Returns:
            The updated task, or None if persistence failed.
        """
        completed_at = None
        started_at = None

        # Calculate timing
        if status == TaskStatus.RUNNING:
            started_at = datetime.utcnow()
        if status in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT):
            completed_at = datetime.utcnow()

        # Persist to database
        async with await get_session_no_context() as session:
            updated = await TaskRepository.update(
                session,
                task.id,
                status=status,
                updated_at=datetime.utcnow(),
                started_at=started_at,
                completed_at=completed_at,
                error_message=error_message,
                result=result,
            )

        if not updated:
            logger.warning(f"Failed to persist final state for task {task.id}")
            return None

        # Update metrics
        if self._enable_metrics:
            self._metrics.total_completed += 1
            if status == TaskStatus.SUCCESS:
                self._metrics.successful_completions += 1
            elif status == TaskStatus.FAILED:
                self._metrics.failed_completions += 1
            elif status == TaskStatus.CANCELLED:
                self._metrics.cancelled_completions += 1
            elif status == TaskStatus.TIMEOUT:
                self._metrics.timeout_completions += 1

        # Dispatch callbacks for terminal states
        if status in (TaskStatus.SUCCESS, TaskStatus.FAILED) and updated.callback_url:
            await self._dispatch_callback(updated, status, result, error_message)

        # Call registered completion handlers
        for handler in self._completion_handlers[status]:
            try:
                handler(updated)
            except Exception:
                logger.exception(f"Completion handler failed for task {task.id}")

        return updated

    async def _dispatch_callback(
        self,
        task: Task,
        status: TaskStatus,
        result: dict[str, Any] | None,
        error_message: str | None,
    ) -> None:
        """Dispatch a completion callback.

        Args:
            task: The completed task.
            status: The final status.
            result: Optional result data.
            error_message: Optional error message.
        """
        try:
            if self._enable_metrics:
                self._metrics.callback_dispatches += 1

            await self.callback_dispatcher.dispatch(
                task.callback_url,
                {
                    "task_id": task.id,
                    "name": task.name,
                    "status": status.value,
                    "result": result,
                    "error_message": error_message,
                    "completed_at": task.completed_at.isoformat() if task.completed_at else None,
                },
            )
            logger.debug(f"Callback dispatched successfully for task {task.id}")
        except Exception as e:
            logger.error(f"Failed to dispatch callback for task {task.id}: {e}", exc_info=True)
            if self._enable_metrics:
                self._metrics.callback_failures += 1

    def register_completion_handler(
        self,
        status: TaskStatus,
        handler: Callable[[Task], None],
    ) -> None:
        """Register a completion handler for a specific status.

        Handlers are called after a task is finalized with the given status.

        Args:
            status: The task status to handle.
            handler: The handler function.
        """
        self._completion_handlers[status].append(handler)

    def unregister_completion_handler(
        self,
        status: TaskStatus,
        handler: Callable[[Task], None],
    ) -> bool:
        """Unregister a completion handler.

        Args:
            status: The task status.
            handler: The handler function to remove.

        Returns:
            True if removed, False if not found.
        """
        try:
            self._completion_handlers[status].remove(handler)
            return True
        except ValueError:
            return False

    def get_metrics(self) -> CompletionMetrics | None:
        """Get the current completion metrics.

        Returns:
            The CompletionMetrics if metrics are enabled, None otherwise.
        """
        return self._metrics

    def reset_metrics(self) -> None:
        """Reset all completion metrics."""
        if self._metrics:
            self._metrics = CompletionMetrics()
