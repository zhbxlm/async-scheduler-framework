"""Task API routes — /api/v1/tasks
aligned with docs/deepwiki-reference/API 参考.md + 任务执行.md
"""
from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from src.api.dependencies import DbSession, get_tenant_context
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
from src.platform.task_state_machine import TaskStateMachine, TaskEvent, InvalidTaskTransition
from src.services.task_application import (
    TaskSubmissionService,
    TaskCancellationService,
    TaskResultService,
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
    tenant_id = ctx.tenant_id or req.tenant_id or "default"
    task_creator = getattr(request.app.state, "task_creator", None)
    if task_creator is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="TaskCreator not initialised (task-api only)",
        )
    try:
        return await TaskSubmissionService(task_creator).submit(req, tenant_id)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid task data: {e}",
        )
    except Exception as e:
        logger.error("Task creation failed", extra={"error_type": type(e).__name__, "error_detail": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Task creation failed unexpectedly. Please retry.",
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
    return await TaskResultService(db).get_result(task_id, ctx.tenant_id, ctx.is_super_admin)


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

    return await TaskCancellationService(db).cancel(task_id, ctx.tenant_id, ctx.is_super_admin)
