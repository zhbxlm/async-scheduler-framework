from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.common.db_utils import maybe_await
from src.models.task import (
    TaskCancelResponse,
    TaskCreate,
    TaskCreateResponse,
    TaskInfo,
    TaskListResponse,
    TaskRecord,
    TaskResultResponse,
    TaskStatus,
    TaskSummary,
)
from src.platform.task_state_machine import InvalidTaskTransition, TaskEvent, TaskStateMachine
from src.services.task_validation import validate_task_artifact, validate_task_scheduling


def _to_json(val: Any) -> dict:
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    try:
        return json.loads(val)
    except Exception:
        return {}


def _serialize(obj: Any) -> str:
    if isinstance(obj, str):
        return obj
    return json.dumps(obj, ensure_ascii=False)


def _to_task_info(task: TaskRecord) -> TaskInfo:
    return TaskInfo(
        task_id=task.task_id,
        tenant_id=task.tenant_id or "",
        task_type=task.task_type or "",
        dag_id=task.dag_id,
        status=task.status,
        priority=task.priority,
        cluster_id=task.cluster_id,
        current_step=task.current_step,
        input_data=_to_json(task.input_data),
        output_data=_to_json(task.output_data),
        metadata_json=_to_json(task.metadata_json),
        callback_url=task.callback_url,
        idempotency_key=task.idempotency_key,
        timeout_seconds=task.timeout_seconds or 3600,
        max_retries=task.max_retries or 3,
        scheduled_at=str(task.scheduled_at) if task.scheduled_at else None,
        cron_expr=task.cron_expr,
        created_at=str(task.created_at) if task.created_at else "",
        updated_at=str(task.updated_at) if task.updated_at else "",
    )


def _to_task_summary(task: TaskRecord) -> TaskSummary:
    return TaskSummary(
        task_id=task.task_id,
        tenant_id=task.tenant_id or "",
        task_type=task.task_type or "",
        status=task.status,
        cluster_id=task.cluster_id or "",
        created_at=str(task.created_at) if task.created_at else "",
        updated_at=str(task.updated_at) if task.updated_at else "",
    )


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
        task = await TaskQueryService(self.session).get_task_record(task_id, tenant_id, is_super_admin)
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
class TaskQueryService:
    session: AsyncSession

    async def list_tasks(
        self,
        *,
        tenant_id: str,
        is_super_admin: bool,
        task_status: TaskStatus | None = None,
        task_type: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> TaskListResponse:
        query = self.session.query(TaskRecord)
        if not is_super_admin:
            query = query.filter(TaskRecord.tenant_id == tenant_id)
        if task_status:
            query = query.filter(TaskRecord.status == task_status)
        if task_type:
            query = query.filter(TaskRecord.task_type == task_type)
        if cursor:
            query = query.filter(TaskRecord.task_id > cursor)
        query = query.order_by(TaskRecord.task_id).limit(limit + 1)
        rows = query.all()

        has_more = len(rows) > limit
        items = rows[:limit]
        next_cursor = items[-1].task_id if has_more and items else None
        return TaskListResponse(items=[_to_task_summary(r) for r in items], next_cursor=next_cursor)

    async def get_task_record(self, task_id: str, tenant_id: str, is_super_admin: bool) -> TaskRecord:
        task = await maybe_await(self.session.get(TaskRecord, task_id))
        if task is None:
            raise HTTPException(status_code=404, detail="Task not found")
        if not is_super_admin and task.tenant_id != tenant_id:
            raise HTTPException(status_code=404, detail="Task not found")
        return task

    async def get_task(self, task_id: str, tenant_id: str, is_super_admin: bool) -> TaskInfo:
        task = await self.get_task_record(task_id, tenant_id, is_super_admin)
        return _to_task_info(task)


@dataclass
class TaskResultService:
    session: AsyncSession

    async def get_result(self, task_id: str, tenant_id: str, is_super_admin: bool) -> TaskResultResponse:
        task = await TaskQueryService(self.session).get_task_record(task_id, tenant_id, is_super_admin)
        if task.status not in (TaskStatus.COMPLETED, TaskStatus.FAILED):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Task is still {task.status.value}; result not yet available")
        output = {}
        if task.output_data:
            try:
                output = json.loads(task.output_data)
            except Exception:
                output = {}
        return TaskResultResponse(task_id=task_id, status=task.status, output_data=output, error_message=task.error_message)
