from __future__ import annotations

from datetime import datetime
from sqlalchemy import DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.common.async_db import Base


class TaskEventRecord(Base):
    __tablename__ = "task_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    run_key: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_payload: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), index=True)

    __table_args__ = (
        Index("ix_task_events_task_time", "task_id", "created_at"),
    )
