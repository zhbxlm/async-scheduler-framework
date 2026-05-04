"""Task API routes — /api/v1/tasks
aligned with docs/deepwiki-reference/API 参考.md + 任务执行.md
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from src.api.dependencies import DbSession, get_tenant_context
from src.models.task import (
    TaskCancelResponse,
    TaskCreate,
    TaskCreateResponse,
    TaskDispatchMode,
    TaskInfo,
    TaskListResponse,
    TaskPriority,
    TaskRecord,
    TaskResultResponse,
    TaskStatus,
    TaskSummary,
)

router = APIRouter(prefix="/tasks", tags=["tasks"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_json(val: Any) -> dict:
    """Deserialise a stored JSON string or return the value as-is."""
    if val is None:
        return {}
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except Exception:
        return {}


def _serialize(obj: Any) -> str:
    """Serialise dict/list to JSON string for ORM storage."""
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
        dispatch_mode=task.dispatch_mode,
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
        dispatch_mode=task.dispatch_mode,
        created_at=str(task.created_at) if task.created_at else "",
        updated_at=str(task.updated_at) if task.updated_at else "",
    )


# ---------------------------------------------------------------------------
# GET /tasks — list with cursor pagination
# ---------------------------------------------------------------------------

@router.get("/", response_model=TaskListResponse, summary="List tasks")
async def list_tasks(
    db: DbSession,
    task_status: TaskStatus | None = Query(default=None, alias="status"),
    task_type: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    cursor: str | None = Query(default=None, description="Opaque pagination cursor (task_id)"),
    ctx=Depends(get_tenant_context),
) -> TaskListResponse:
    query = db.query(TaskRecord)
    if not ctx.is_super_admin:
        query = query.filter(TaskRecord.tenant_id == ctx.tenant_id)
    if task_status:
        query = query.filter(TaskRecord.status == task_status)
    if task_type:
        query = query.filter(TaskRecord.task_type == task_type)
    if cursor:
        # cursor is the task_id of the last item from the previous page
        query = query.filter(TaskRecord.task_id > cursor)
    query = query.order_by(TaskRecord.task_id).limit(limit + 1)
    rows = query.all()

    has_more = len(rows) > limit
    items = rows[:limit]
    next_cursor = items[-1].task_id if has_more and items else None
    return TaskListResponse(
        items=[_to_task_summary(r) for r in items],
        next_cursor=next_cursor,
    )


# ---------------------------------------------------------------------------
# POST /tasks — create task
# ---------------------------------------------------------------------------

@router.post(
    "/",
    response_model=TaskCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit task",
)
async def create_task(
    req: TaskCreate,
    db: DbSession,
    ctx=Depends(get_tenant_context),
) -> TaskCreateResponse:
    tenant_id = ctx.tenant_id or req.tenant_id or "default"

    # Idempotency check
    if req.idempotency_key:
        existing = (
            db.query(TaskRecord)
            .filter(
                TaskRecord.tenant_id == tenant_id,
                TaskRecord.idempotency_key == req.idempotency_key,
            )
            .first()
        )
        if existing:
            return TaskCreateResponse(
                task_id=existing.task_id,
                tenant_id=existing.tenant_id or "",
                status=existing.status,
                dispatch_mode=existing.dispatch_mode,
                cluster_id=existing.cluster_id or "",
                idempotent_reused=True,
            )

    task_id = f"task-{uuid.uuid4().hex[:12]}"
    dispatch_mode = req.dispatch_mode or TaskDispatchMode.DAG_ORCHESTRATED

    task = TaskRecord(
        task_id=task_id,
        tenant_id=tenant_id,
        task_type=req.task_type,
        status=TaskStatus.PENDING,
        priority=req.priority,
        dispatch_mode=dispatch_mode,
        input_data=_serialize(req.input_data),
        metadata_json=_serialize(req.metadata),
        callback_url=req.callback_url,
        idempotency_key=req.idempotency_key,
        timeout_seconds=req.timeout_seconds,
        max_retries=req.max_retries,
        cron_expr=req.cron_expr,
    )
    db.add(task)
    db.commit()
    db.refresh(task)
    return TaskCreateResponse(
        task_id=task.task_id,
        tenant_id=task.tenant_id or "",
        status=task.status,
        dispatch_mode=task.dispatch_mode,
        cluster_id=task.cluster_id or "",
        idempotent_reused=False,
    )


# ---------------------------------------------------------------------------
# GET /tasks/{task_id} — get full task detail
# ---------------------------------------------------------------------------

@router.get("/{task_id}", response_model=TaskInfo, summary="Get task detail")
async def get_task(
    task_id: str,
    db: DbSession,
    ctx=Depends(get_tenant_context),
) -> TaskInfo:
    task = db.query(TaskRecord).filter(TaskRecord.task_id == task_id).first()
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id!r} not found")
    if not ctx.is_super_admin and task.tenant_id != ctx.tenant_id:
        raise HTTPException(status_code=403, detail="Access denied")
    return _to_task_info(task)


# ---------------------------------------------------------------------------
# GET /tasks/{task_id}/result — retrieve task result
# ---------------------------------------------------------------------------

@router.get("/{task_id}/result", response_model=TaskResultResponse, summary="Get task result")
async def get_task_result(
    task_id: str,
    db: DbSession,
    ctx=Depends(get_tenant_context),
) -> TaskResultResponse:
    task = db.query(TaskRecord).filter(TaskRecord.task_id == task_id).first()
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id!r} not found")
    if not ctx.is_super_admin and task.tenant_id != ctx.tenant_id:
        raise HTTPException(status_code=403, detail="Access denied")
    if task.status not in (TaskStatus.COMPLETED, TaskStatus.FAILED):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Task is still {task.status.value}; result not yet available",
        )
    return TaskResultResponse(
        task_id=task.task_id,
        status=task.status,
        output_data=_to_json(task.output_data),
    )


# ---------------------------------------------------------------------------
# DELETE /tasks/{task_id} — cancel task
# ---------------------------------------------------------------------------

@router.delete(
    "/{task_id}",
    response_model=TaskCancelResponse,
    summary="Cancel task",
)
async def cancel_task(
    task_id: str,
    db: DbSession,
    ctx=Depends(get_tenant_context),
) -> TaskCancelResponse:
    task = db.query(TaskRecord).filter(TaskRecord.task_id == task_id).first()
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id!r} not found")
    if not ctx.is_super_admin and task.tenant_id != ctx.tenant_id:
        raise HTTPException(status_code=403, detail="Access denied")

    prior = task.status
    terminal = {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}
    if prior in terminal:
        return TaskCancelResponse(task_id=task_id, cancelled=False, prior_status=prior)

    task.status = TaskStatus.CANCELLED
    db.commit()
    return TaskCancelResponse(task_id=task_id, cancelled=True, prior_status=prior)
