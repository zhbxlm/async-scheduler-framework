"""SQLAlchemy ORM models for persistence."""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Enum as SQLEnum,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from async_scheduler.core.models import (
    DAGExecutionStatus,
    ScheduleStatus,
    TaskPriority,
    TaskStatus,
)


class Base(DeclarativeBase):
    """Base class for all ORM models."""

    pass


class TenantORM(Base):
    """Tenant ORM model."""

    __tablename__ = "tenants"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    config: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class TaskORM(Base):
    """Task ORM model."""

    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[TaskStatus] = mapped_column(SQLEnum(TaskStatus), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    priority: Mapped[TaskPriority] = mapped_column(SQLEnum(TaskPriority), nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=300)
    callback_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    tenant_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    tags: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class ScheduleORM(Base):
    """Schedule ORM model."""

    __tablename__ = "schedules"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    cron_expression: Mapped[str] = mapped_column(String(100), nullable=False)
    task_template: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    tenant_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    timezone: Mapped[str] = mapped_column(String(50), nullable=False, default="UTC")
    dedup_window_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    status: Mapped[ScheduleStatus] = mapped_column(SQLEnum(ScheduleStatus), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class DAGNodeORM(Base):
    """DAG Node ORM model."""

    __tablename__ = "dag_nodes"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    dag_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    task_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    dependencies: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    condition: Mapped[str | None] = mapped_column(Text, nullable=True)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=300)
    on_failure: Mapped[str] = mapped_column(String(20), nullable=False, default="fail")
    fallback_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class DAGExecutionORM(Base):
    """DAG Execution ORM model."""

    __tablename__ = "dag_executions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[DAGExecutionStatus] = mapped_column(SQLEnum(DAGExecutionStatus), nullable=False)
    tenant_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    max_parallelism: Mapped[int] = mapped_column(Integer, nullable=False, default=4)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    node_executions: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    context: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)


if TYPE_CHECKING:
    # For type checking, expose relationships
    pass
