"""Task router for lifecycle orchestration and queue admission."""

from __future__ import annotations

from datetime import datetime, timedelta

from async_scheduler.core.models import Task, TaskCreate, TaskStatus
from async_scheduler.persistence import TaskRepository, get_session_no_context
from async_scheduler.platform.quota import TenantQuotaManager
from async_scheduler.queue import QueueManager


class TaskRouter:
    """Creates tasks, applies scheduling rules, and enqueues them."""

    def __init__(self, queue_manager: QueueManager, quota_manager: TenantQuotaManager | None = None) -> None:
        self.queue_manager = queue_manager
        self.quota_manager = quota_manager

    async def create_task(self, task_create: TaskCreate) -> Task:
        if self.quota_manager is not None:
            self.quota_manager.admit_queue(task_create.tenant_id)

        async with await get_session_no_context() as session:
            task = await TaskRepository.create(session, task_create)
            scheduled_at = task.scheduled_at

            # P1-TODO-6: infer capability from task metadata
            # Checks tags list for items starting with "capability:", else "default"
            capability = "default"
            for tag in (task.tags or []):
                if isinstance(tag, str) and tag.startswith("capability:"):
                    capability = tag.split(":", 1)[1].strip()
                    break

            if scheduled_at and scheduled_at > datetime.utcnow():
                task = await TaskRepository.update(session, task.id, status=TaskStatus.QUEUED) or task
                await self.queue_manager.enqueue(task, scheduled_at=scheduled_at, capability=capability)
            else:
                task = await TaskRepository.update(session, task.id, status=TaskStatus.QUEUED) or task
                await self.queue_manager.enqueue(task, capability=capability)

            return task

    async def create_delayed_task(self, task_create: TaskCreate, delay_seconds: int) -> Task:
        scheduled_at = datetime.utcnow() + timedelta(seconds=delay_seconds)
        return await self.create_task(task_create.model_copy(update={"scheduled_at": scheduled_at}))
