"""Schedule domain models — aligned with docs/deepwiki-reference/数据模型.md"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import Boolean, DateTime, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.common.async_db import Base


# ---------------------------------------------------------------------------
# ORM Model
# ---------------------------------------------------------------------------

class ScheduleRecord(Base):
    """Cron schedule record mapped to 'schedules' table."""
    __tablename__ = "schedules"

    schedule_id: Mapped[str] = mapped_column(
        String(64), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    tenant_id: Mapped[str] = mapped_column(
        String(64), nullable=False, default="", index=True
    )
    cron_expr: Mapped[str] = mapped_column(String(64), nullable=False)
    task_template: Mapped[str] = mapped_column(Text, nullable=False)  # TaskCreate JSON
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    last_triggered_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    next_fire_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )


# ---------------------------------------------------------------------------
# API Transfer Models
# ---------------------------------------------------------------------------

class ScheduleCreate(BaseModel):
    """Request body for Cron schedule creation."""
    tenant_id: str = ""
    cron_expr: str
    task_type: str
    input_data: dict[str, Any] = Field(default_factory=dict)
    priority: str = "normal"
    callback_url: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = 3600
    max_retries: int = 3
    enabled: bool = True


class ScheduleInfo(BaseModel):
    """Full schedule information schema."""
    model_config = ConfigDict(from_attributes=True)

    schedule_id: str = ""
    tenant_id: str = ""
    cron_expr: str = ""
    enabled: bool = True
    last_triggered_at: str | None = None
    next_fire_at: str | None = None
    created_at: str = ""
    updated_at: str = ""

    # Expanded task template fields
    task_type: str = ""
    input_data: dict[str, Any] = Field(default_factory=dict)
    priority: str = "normal"
    callback_url: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = 3600
    max_retries: int = 3

    @model_validator(mode="before")
    @classmethod
    def fix_lua_cjson_empty_tables(cls, values: Any) -> Any:
        if isinstance(values, dict):
            for key in ("input_data", "metadata"):
                if isinstance(values.get(key), list) and len(values[key]) == 0:
                    values[key] = {}
        return values
