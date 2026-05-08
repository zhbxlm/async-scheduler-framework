"""Task API routes — /api/v1/tasks
aligned with docs/deepwiki-reference/API 参考.md + 任务执行.md
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from src.api.dependencies import DbSession, get_tenant_context
from src.models.task import (
    TaskCancelResponse,
    TaskCreate,
    TaskCreateResponse,
    TaskInfo,
    TaskListResponse,
    TaskResultResponse,
    TaskStatus,
)
from src.services.task_application import (
    TaskCancellationService,
    TaskQueryService,
    TaskResultService,
    TaskSubmissionService,
)
from src.services.task_application import (
    _serialize as _service_serialize,
)
from src.services.task_application import (
    _to_json as _service_to_json,
)

router = APIRouter(prefix="/tasks", tags=["tasks"])

logger = logging.getLogger(__name__)


def _to_json(val):
    return _service_to_json(val)


def _serialize(obj):
    return _service_serialize(obj)


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
    return await TaskQueryService(db).list_tasks(
        tenant_id=ctx.tenant_id,
        is_super_admin=ctx.is_super_admin,
        task_status=task_status,
        task_type=task_type,
        limit=limit,
        cursor=cursor,
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
    return await TaskQueryService(db).get_task(task_id, ctx.tenant_id, ctx.is_super_admin)


# ---------------------------------------------------------------------------
# GET /tasks/{task_id}/result — retrieve task result
# ---------------------------------------------------------------------------

@router.get("/{task_id}/result", response_model=TaskResultResponse, summary="Get task result")
async def get_task_result(
    task_id: str,
    db: DbSession,
    ctx=Depends(get_tenant_context),
) -> TaskResultResponse:
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
    return await TaskCancellationService(db).cancel(task_id, ctx.tenant_id, ctx.is_super_admin)
