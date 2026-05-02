"""Core domain models for the async scheduler."""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class TaskStatus(str, Enum):
    """Task execution status.

    Aligned with deepwiki ray-amu task state machine.
    SCHEDULED is added to represent tasks awaiting their scheduled_at time.
    """

    PENDING = "pending"
    SCHEDULED = "scheduled"  # has future scheduled_at; waiting in delayed queue
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    RETRY = "retry"

    @property
    def is_terminal(self) -> bool:
        return self in {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT}


class ExecutionAttemptStatus(str, Enum):
    """Distributed execution-attempt status."""

    CLAIMED = "claimed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ABANDONED = "abandoned"


class TaskPriority(int, Enum):
    """Task priority levels.

    Aligned with deepwiki ray-amu spec.
    ``priority_rank`` is the sort key used in queue score encoding:
    score = priority_rank * 10**13 + timestamp_ms
    Smaller rank ⇒ higher priority (VERY_HIGH = 1, TIDE = 5).
    """

    VERY_HIGH = 1   # rank 1 – highest
    HIGH = 2        # rank 2
    NORMAL = 3      # rank 3 – default
    LOW = 4         # rank 4
    TIDE = 5        # rank 5 – lowest (tidal / best-effort)

    # Legacy aliases kept for backward compatibility
    CRITICAL = 1    # maps to VERY_HIGH

    @property
    def priority_rank(self) -> int:
        """Sort rank used in Redis score encoding (1 = highest priority)."""
        return self.value

    @property
    def label(self) -> str:
        return self.name.lower()


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
    PARTIAL = "partial"


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


class ExecutionAttemptBase(BaseModel):
    """Base execution-attempt model."""

    model_config = ConfigDict(from_attributes=True)

    task_id: str
    worker_id: str
    retry_index: int = Field(default=0, ge=0)
    lease_token: str


class ExecutionAttempt(ExecutionAttemptBase):
    """Full execution-attempt model."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    status: ExecutionAttemptStatus = ExecutionAttemptStatus.CLAIMED
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    error_message: str | None = None
    result_payload: dict[str, Any] | None = None


class ExecutionAttemptCreate(ExecutionAttemptBase):
    """Execution-attempt creation request."""

    pass


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
    task_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    dependencies: list[str] = Field(default_factory=list)
    condition: str | None = None
    retry_count: int = 0
    max_retries: int = 3
    timeout_seconds: int = 300
    on_failure: str = "fail"
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
    context: dict[str, Any] = Field(default_factory=dict)


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
