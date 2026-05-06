"""Task API routes — /api/v1/tasks
aligned with docs/deepwiki-reference/API 参考.md + 任务执行.md
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel

from src.api.dependencies import DbSession, get_tenant_context, get_db_session
from src.models.task import (
    TaskCancelResponse,
    TaskCreate,
    TaskCreateResponse,
    TaskInfo,
    TaskListResponse,
    TaskPriority,
    TaskRecord,
    TaskResultResponse,
    TaskStatus,
    TaskSummary,
)
from src.services.task_validation import (
    validate_task_artifact,
    validate_task_scheduling,
    fix_lua_cjson_empty_tables,
)

router = APIRouter(prefix="/tasks", tags=["tasks"])

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_json(val: Any) -> dict:
    """Deserialise a stored JSON string or return the value as-is.
    
    Optimised: assumes data was normalised at write time.
    """
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    # Fallback for legacy data stored as JSON strings
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
# POST /tasks — create task (via TaskCreator)
# ---------------------------------------------------------------------------

@router.post(
    "/",
    response_model=TaskCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a new task",
    description="""
    Create and enqueue a new asynchronous task.
    
    **Idempotency**: If `idempotency_key` is provided, duplicate requests with the
    same key will return the existing task (no new task created).
    
    **Priority**: Tasks are processed in priority order (1=highest, 5=lowest).
    
    **Queue Position**: The returned `queue_position` indicates the task's
    position in the queue for wait time estimation.
    """,
    responses={
        201: {"description": "Task created successfully"},
        409: {"description": "Duplicate task (idempotency_key conflict)"},
        422: {"description": "Invalid task data"},
        503: {"description": "Queue capacity exceeded or service unavailable"},
    },
)
async def create_task(
    req: TaskCreate,
    request: Request,
    db: DbSession,
    ctx=Depends(get_tenant_context),
) -> TaskCreateResponse:
    """Create and enqueue a task via TaskCreator (owns idempotency)."""
    # Validate task data using service layer
    try:
        validate_task_artifact(req.artifact_url, req.artifact_sha256)
        validate_task_scheduling(req.scheduled_at, req.delay_seconds)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid task data: {e}",
        )
    
    tenant_id = ctx.tenant_id or req.tenant_id or "default"
    task_creator = getattr(request.app.state, "task_creator", None)
    if task_creator is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="TaskCreator not initialised (task-api only)",
        )

    # Convert to TaskCreator kwargs (TaskCreator handles idempotency)
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

    try:
        result = await task_creator.create_task(
            tenant_id=tenant_id,
            idempotency_key=req.idempotency_key,
            **kwargs,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid task data: {e}",
        )
    except DuplicateTaskError as e:  # noqa: F821
        # Idempotency key conflict - return existing task info
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        )
    except QueueCapacityError as e:  # noqa: F821
        # Queue at capacity
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Queue capacity exceeded for capability '{req.capability}': {e}. Please retry later.",
        )
    except Exception as e:
        # Log the actual error for debugging
        logger.error(
            "Task creation failed",
            extra={
                "task_id": req.task_id,
                "capability": req.capability,
                "error_type": type(e).__name__,
                "error_detail": str(e),
            }
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Task creation failed unexpectedly. Please retry.",
        )

    # Map result to TaskCreateResponse
    return TaskCreateResponse(
        task_id=result["task_id"],
        tenant_id=tenant_id,
        status=TaskStatus.QUEUED,
        cluster_id="",  # Not yet assigned
        estimated_wait_seconds=0,
        queue_position=result.get("queue_position", -1),
        scheduled_at="",
        idempotent_reused=result.get("idempotent_reused", False),
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
