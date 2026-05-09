"""task-api package-owned FastAPI dependencies."""
from __future__ import annotations

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Annotated, Any

from fastapi import Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from scheduler_task_api.auth import authenticate
from scheduler_task_api.async_db import get_async_db


@dataclass
class TenantContext:
    tenant_id: str
    api_key: str = ""
    is_super_admin: bool = False
    quota_max_tasks: int = 0
    quota_max_gpus: int = 0
    quota_max_actors: int = 0


async def get_tenant_context(auth_result: dict = Depends(authenticate)):
    return TenantContext(
        tenant_id=auth_result["tenant_id"],
        api_key=auth_result.get("api_key", ""),
        is_super_admin=auth_result.get("is_super_admin", False),
    )


async def get_db_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    session_factory = getattr(request.app.state, "async_session_factory", None)
    if session_factory is None:
        raise RuntimeError(
            "Database not configured. Ensure async_session_factory is set in app.state during lifespan startup."
        )
    async with get_async_db(session_factory) as session:
        yield session


DbSession = Annotated[AsyncSession, Depends(get_db_session)]


def _get_app_state_attr(request: Request, attr_name: str, detail: str) -> Any:
    value = getattr(request.app.state, attr_name, None)
    if value is None:
        raise HTTPException(status_code=503, detail=detail)
    return value
