"""Core domain models for the async scheduler."""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class TaskStatus(str, Enum):
    """Task execution status."""

    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    RETRY = "retry"


class TaskPriority(int, Enum):
    """Task priority levels."""

    LOW = 1
    NORMAL = 5
    HIGH = 10
    CRITICAL = 20


class ScheduleStatus(str, Enum):
    """Schedule status."""

    ACTIVE = "active"
    PAUSED = "paused"
    DISABLED = "disabled"


class DAGExecutionStatus(str, Enum):
    """DAG execution status."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PARTIAL = "partial"  # Some steps succeeded, some failed


class TaskBase(BaseModel):
    """Base task model."""

    model_config = ConfigDict(from_attributes=True)

    name: str = Field(..., min_length=1, max_length=255)
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    max_retries: int = Field(default=3, ge=0)
    timeout_seconds: int = Field(default=300, ge=1)
    callback_url: str | None = None
    scheduled_at: datetime | None = None
    tenant_id: str | None = None
    idempotency_key: str | None = None
    tags: list[str] = Field(default_factory=list)


class Task(TaskBase):
    """Full task model with runtime fields."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    retry_count: int = 0
    error_message: str | None = None
    result: dict[str, Any] | None = None


class TaskCreate(TaskBase):
    """Task creation request."""

    pass


class TaskUpdate(BaseModel):
    """Task update request."""

    name: str | None = None
    payload: dict[str, Any] | None = None
    priority: TaskPriority | None = None
    status: TaskStatus | None = None
    error_message: str | None = None
    result: dict[str, Any] | None = None


class ScheduleBase(BaseModel):
    """Base schedule model."""

    model_config = ConfigDict(from_attributes=True)

    name: str = Field(..., min_length=1, max_length=255)
    cron_expression: str = Field(..., pattern=r"^[\d*/\-,\s]+$")
    task_template: dict[str, Any] = Field(default_factory=dict)
    tenant_id: str | None = None
    timezone: str = "UTC"
    dedup_window_seconds: int = Field(default=60, ge=0)


class Schedule(ScheduleBase):
    """Full schedule model with runtime fields."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    status: ScheduleStatus = ScheduleStatus.ACTIVE
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_run_at: datetime | None = None
    next_run_at: datetime | None = None


class ScheduleCreate(ScheduleBase):
    """Schedule creation request."""

    pass


class DAGNode(BaseModel):
    """A node in a DAG representing a task step."""

    model_config = ConfigDict(from_attributes=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    task_type: str  # Type identifier for worker routing
    payload: dict[str, Any] = Field(default_factory=dict)
    dependencies: list[str] = Field(default_factory=list)  # Node IDs this depends on
    condition: str | None = None  # Python expression for conditional execution
    retry_count: int = 0
    max_retries: int = 3
    timeout_seconds: int = 300
    on_failure: str = "fail"  # "fail", "skip", "fallback"
    fallback_payload: dict[str, Any] | None = None


class DAGNodeExecution(BaseModel):
    """Execution state for a DAG node."""

    node_id: str
    status: TaskStatus = TaskStatus.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    retry_count: int = 0
    error_message: str | None = None
    result: dict[str, Any] | None = None
    skipped: bool = False
    skip_reason: str | None = None


class DAGBase(BaseModel):
    """Base DAG model."""

    model_config = ConfigDict(from_attributes=True)

    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    nodes: list[DAGNode] = Field(default_factory=list)
    tenant_id: str | None = None
    max_parallelism: int = Field(default=4, ge=1)


class DAG(DAGBase):
    """Full DAG model with runtime fields."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    status: DAGExecutionStatus = DAGExecutionStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    node_executions: dict[str, DAGNodeExecution] = Field(default_factory=dict)
    context: dict[str, Any] = Field(default_factory=dict)  # Shared execution context


class DAGCreate(DAGBase):
    """DAG creation request."""

    pass


class Tenant(BaseModel):
    """Tenant for multi-tenancy support."""

    model_config = ConfigDict(from_attributes=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(..., min_length=1, max_length=255)
    config: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = True


class TenantCreate(BaseModel):
    """Tenant creation request."""

    name: str = Field(..., min_length=1, max_length=255)
    config: dict[str, Any] = Field(default_factory=dict)


class TenantUpdate(BaseModel):
    """Tenant update request."""

    config: dict[str, Any] = Field(default_factory=dict)