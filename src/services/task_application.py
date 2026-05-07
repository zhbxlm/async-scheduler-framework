from __future__ import annotations


from dataclasses import dataclass
import json
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.common.db_utils import maybe_await
from src.models.task import (
    TaskRecord,
    TaskStatus,
    TaskCreate,
    TaskCreateResponse,
    TaskResultResponse,
    TaskCancelResponse,
)
from src.platform.task_state_machine import TaskStateMachine, TaskEvent, InvalidTaskTransition
from src.services.task_validation import validate_task_artifact, validate_task_scheduling




@dataclass
class TaskSubmissionService:
    task_creator: Any

    async def submit(self, req: TaskCreate, tenant_id: str) -> TaskCreateResponse:
        validate_task_artifact(req.artifact_url, req.artifact_sha256)
        validate_task_scheduling(req.scheduled_at, req.delay_seconds)
        kwargs = {
            "task_type": req.task_type,
            "priority": req.priority.value,
            "input_data": req.input_data,
            "metadata": req.metadata,
            "callback_url": req.callback_url,
            "timeout_seconds": req.timeout_seconds,
            "max_retries": req.max_retries,
            "cron_expr": req.cron_expr,
            "persist_to_db": True,
        }
        if req.scheduled_at:
            kwargs["scheduled_at"] = req.scheduled_at
        if req.delay_seconds:
            kwargs["delay_seconds"] = req.delay_seconds
        if req.artifact_url:
            kwargs["artifact_url"] = req.artifact_url
        if req.artifact_sha256:
            kwargs["artifact_sha256"] = req.artifact_sha256
        result = await self.task_creator.create_task(tenant_id=tenant_id, idempotency_key=req.idempotency_key, **kwargs)
        transition = TaskStateMachine.transition(TaskStatus.PENDING, TaskEvent.ENQUEUE)
        return TaskCreateResponse(
            task_id=result["task_id"],
            tenant_id=tenant_id,
            status=transition.current,
            cluster_id="",
            estimated_wait_seconds=0,
            queue_position=result.get("queue_position", -1),
            scheduled_at="",
            idempotent_reused=result.get("idempotent_reused", False),
        )


@dataclass
class TaskCancellationService:
    session: AsyncSession

    async def cancel(self, task_id: str, tenant_id: str, is_super_admin: bool) -> TaskCancelResponse:
        task = await maybe_await(self.session.get(TaskRecord, task_id))
        if task is None:
            raise HTTPException(status_code=404, detail="Task not found")
        if not is_super_admin and task.tenant_id != tenant_id:
            raise HTTPException(status_code=404, detail="Task not found")
        prior = task.status
        terminal = {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}
        if prior in terminal:
            return TaskCancelResponse(task_id=task_id, cancelled=False, prior_status=prior)
        try:
            task.status = TaskStateMachine.transition(prior, TaskEvent.CANCEL).current
        except InvalidTaskTransition:
            return TaskCancelResponse(task_id=task_id, cancelled=False, prior_status=prior)
        await maybe_await(self.session.commit())
        return TaskCancelResponse(task_id=task_id, cancelled=True, prior_status=prior)


@dataclass
class TaskResultService:
    session: AsyncSession

    async def get_result(self, task_id: str, tenant_id: str, is_super_admin: bool) -> TaskResultResponse:
        task = await maybe_await(self.session.get(TaskRecord, task_id))
        if task is None:
            raise HTTPException(status_code=404, detail="Task not found")
        if not is_super_admin and task.tenant_id != tenant_id:
            raise HTTPException(status_code=404, detail="Task not found")
        if task.status not in (TaskStatus.COMPLETED, TaskStatus.FAILED):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Task is still {task.status.value}; result not yet available")
        output = {}
        if task.output_data:
            try:
                output = json.loads(task.output_data)
            except Exception:
                output = {}
        return TaskResultResponse(task_id=task_id, status=task.status, output_data=output, error_message=task.error_message)
