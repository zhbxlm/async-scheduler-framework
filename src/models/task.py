"""Task domain models — aligned with docs/deepwiki-reference/数据模型.md"""
from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import Boolean, DateTime, Enum as SAEnum, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.common.async_db import Base


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class TaskStatus(str, Enum):
    """Task lifecycle status."""
    PENDING = "pending"
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskPriority(str, Enum):
    """Task priority — lower weight = higher priority."""
    VERY_HIGH = "very_high"   # weight 1
    HIGH = "high"             # weight 2
    NORMAL = "normal"         # weight 3 (default)
    LOW = "low"               # weight 4
    TIDE = "tide"             # weight 5

    @property
    def weight(self) -> int:
        return {"very_high": 1, "high": 2, "normal": 3, "low": 4, "tide": 5}[self.value]


# ---------------------------------------------------------------------------
# ORM Model
# ---------------------------------------------------------------------------

class TaskRecord(Base):
    """Persistent task record mapped to 'tasks' table."""
    __tablename__ = "tasks"

    # Identification
    task_id: Mapped[str] = mapped_column(
        String(64), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    tenant_id: Mapped[str] = mapped_column(
        String(64), nullable=False, default="", index=True
    )
    task_type: Mapped[str] = mapped_column(String(128), nullable=False, index=True)

    # DAG & scheduling
    dag_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[TaskStatus] = mapped_column(
        SAEnum(TaskStatus), nullable=False, default=TaskStatus.PENDING, index=True
    )
    priority: Mapped[TaskPriority] = mapped_column(
        SAEnum(TaskPriority), nullable=False, default=TaskPriority.NORMAL
    )
    cluster_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    current_step: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # Data
    input_data: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_data: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Control
    callback_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(
        String(128), nullable=True, index=True
    )
    timeout_seconds: Mapped[int] = mapped_column(Integer, default=3600)
    max_retries: Mapped[int] = mapped_column(Integer, default=3)
    attempt: Mapped[int] = mapped_column(Integer, default=0)
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    cron_expr: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    # Composite indexes for common query patterns
    __table_args__ = (
        # Hot query: tenant tasks by status (list endpoint)
        Index("ix_tasks_tenant_status", "tenant_id", "status"),
        # Hot query: tenant tasks ordered by creation time (pagination)
        Index("ix_tasks_tenant_created", "tenant_id", "created_at"),
        # Reconciler: find stuck/running tasks quickly
        Index("ix_tasks_status_updated", "status", "updated_at"),
        # Cron: find scheduled tasks due for execution
        Index("ix_tasks_scheduled_at", "scheduled_at"),
        # DAG queries: find all tasks for a DAG
        Index("ix_tasks_dag_id", "dag_id"),
    )


# ---------------------------------------------------------------------------
# API Transfer Models
# ---------------------------------------------------------------------------

class TaskCreate(BaseModel):
    """Request body for task creation."""
    tenant_id: str = ""
    task_type: str = Field(..., min_length=1, max_length=128)
    priority: TaskPriority = TaskPriority.NORMAL

    # Data
    input_data: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    # Control
    callback_url: str | None = None
    idempotency_key: str | None = None
    timeout_seconds: int = Field(default=3600, ge=1, le=86400)
    max_retries: int = Field(default=3, ge=0, le=20)

    # Scheduling
    scheduled_at: str | None = None
    delay_seconds: int | None = None
    cron_expr: str | None = None

    # Remote code
    artifact_url: str | None = None
    artifact_sha256: str | None = None


class TaskInfo(BaseModel):
    """Full task information schema for query responses."""
    model_config = ConfigDict(from_attributes=True)

    task_id: str = ""
    tenant_id: str = ""
    task_type: str = ""
    dag_id: str | None = None
    status: TaskStatus = TaskStatus.PENDING
    priority: TaskPriority = TaskPriority.NORMAL
    cluster_id: str | None = None
    current_step: str | None = None
    input_data: dict[str, Any] = Field(default_factory=dict)
    output_data: dict[str, Any] = Field(default_factory=dict)
    error_message: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    callback_url: str | None = None
    idempotency_key: str | None = None
    timeout_seconds: int = 3600
    max_retries: int = 3
    scheduled_at: str | None = None
    cron_expr: str | None = None
    created_at: str = ""
    updated_at: str = ""

    # Extra fields
    attempt: int = 0
    artifact_url: str = ""
    artifact_sha256: str = ""




class TaskCreateResponse(BaseModel):
    """Response body after successful task creation."""
    task_id: str
    tenant_id: str = ""
    status: TaskStatus
    cluster_id: str = ""
    estimated_wait_seconds: int = 0
    queue_position: int = 0
    scheduled_at: str = ""
    idempotent_reused: bool = False


class TaskResultResponse(BaseModel):
    """Response body for task result queries."""
    task_id: str
    status: TaskStatus
    output_data: dict[str, Any] = Field(default_factory=dict)
    error_message: str | None = None


class TaskSummary(BaseModel):
    """Task summary schema for list endpoints (omits large fields)."""
    model_config = ConfigDict(from_attributes=True)

    task_id: str
    tenant_id: str = ""
    task_type: str = ""
    status: TaskStatus = TaskStatus.PENDING
    cluster_id: str = ""
    created_at: str = ""
    updated_at: str = ""
    attempt: int = 0


class TaskListResponse(BaseModel):
    """Cursor-paginated task list response."""
    items: list[TaskSummary] = Field(default_factory=list)
    next_cursor: str | None = None


class TaskCancelResponse(BaseModel):
    """Task cancellation response."""
    task_id: str
    cancelled: bool
    prior_status: TaskStatus
