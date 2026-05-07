from __future__ import annotations

from datetime import datetime
from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.common.async_db import Base


class TaskRunRecord(Base):
    """Execution-instance layer for a concrete task run.

    P1 minimal model: distinguishes durable task definition/identity from an
    execution attempt lineage record.
    """
    __tablename__ = "task_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(String(64), ForeignKey("tasks.task_id"), nullable=False, index=True)
    run_key: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="created", index=True)
    worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index("ix_task_runs_task_attempt", "task_id", "attempt"),
    )
