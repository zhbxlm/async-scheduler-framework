"""Task API routes."""
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from typing import List

from src.api.dependencies import DbSession, get_tenant_context
from src.models.task import TaskRecord, TaskStatus, TaskCreate

router = APIRouter(prefix="/tasks", tags=["tasks"])


class TaskResponse(BaseModel):
    task_id: str
    status: str
    created_at: str


@router.get("/", response_model=List[TaskResponse])
async def list_tasks(
    db: DbSession,
    status: TaskStatus | None = None,
    limit: int = 100,
    ctx=Depends(get_tenant_context),
):
    query = db.query(TaskRecord)
    if status:
        query = query.filter(TaskRecord.status == status)
    query = query.order_by(TaskRecord.created_at.desc()).limit(limit)
    return query.all()


@router.post("/", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
async def create_task(
    req: TaskCreate,
    db: DbSession,
    ctx=Depends(get_tenant_context),
):
    task = TaskRecord(
        task_id=req.task_id,
        status=TaskStatus.PENDING,
        payload=req.payload,
        tenant_id=ctx.tenant_id,
    )
    db.add(task)
    db.commit()
    db.refresh(task)
    return task
