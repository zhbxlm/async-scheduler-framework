"""Tenant domain models — aligned with docs/deepwiki-reference/数据模型.md"""
from __future__ import annotations

import secrets
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator


class TenantStatus(str, Enum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    DELETED = "deleted"


class TenantQuota(BaseModel):
    """Tenant resource quota (0 = unlimited)."""
    max_gpus: int = 0
    max_queue_depth: int = 0
    max_concurrent_tasks: int = 0
    max_actor_count: int = 0

    @model_validator(mode="before")
    @classmethod
    def fix_lua_cjson_empty_tables(cls, values: Any) -> Any:
        """Lua cjson serialises empty {} as [] — convert back to 0."""
        if isinstance(values, dict):
            for key in ("max_gpus", "max_queue_depth", "max_concurrent_tasks", "max_actor_count"):
                if isinstance(values.get(key), list):
                    values[key] = 0
        return values


class TenantInfo(BaseModel):
    """Full tenant registration info."""
    tenant_id: str = Field(
        default_factory=lambda: f"tenant-{secrets.token_hex(6)}"
    )
    tenant_name: str = ""
    status: TenantStatus = TenantStatus.ACTIVE
    quota: TenantQuota = Field(default_factory=TenantQuota)
    description: str = ""
    created_at: str = ""
    updated_at: str = ""
