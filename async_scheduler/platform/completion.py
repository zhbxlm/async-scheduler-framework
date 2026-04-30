"""Task completion node.

Separates final-state persistence and callback dispatch from the consumer loop,
closer to the deepwiki task completion node concept.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from async_scheduler.core.models import Task, TaskStatus
from async_scheduler.persistence import TaskRepository, get_session_no_context
from async_scheduler.platform.callback import CallbackDispatcher


class TaskCompletionNode:
    def __init__(self, callback_dispatcher: CallbackDispatcher | None = None) -> None:
        self.callback_dispatcher = callback_dispatcher or CallbackDispatcher()

    async def finalize(
        self,
        task: Task,
        status: TaskStatus,
        *,
        error_message: str | None = None,
        result: dict[str, Any] | None = None,
    ) -> Task | None:
        completed_at = None
        started_at = None
        if status == TaskStatus.RUNNING:
            started_at = datetime.utcnow()
        if status in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT):
            completed_at = datetime.utcnow()

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

        if updated and status in (TaskStatus.SUCCESS, TaskStatus.FAILED):
            await self.callback_dispatcher.dispatch(
                updated.callback_url,
                {
                    "task_id": updated.id,
                    "status": updated.status.value,
                    "result": updated.result,
                    "error_message": updated.error_message,
                },
            )

        return updated
