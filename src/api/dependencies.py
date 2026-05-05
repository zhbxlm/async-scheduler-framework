"""FastAPI dependencies — auth, tenant context, DB session."""
from __future__ import annotations
from fastapi import Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Annotated, AsyncGenerator

from src.api.auth import authenticate
from src.common.async_db import get_async_db


async def get_tenant_context(auth_result: dict = Depends(authenticate)):
    """Extract tenant context from auth result."""
    from src.models.tenant_context import TenantContext
    return TenantContext(
        tenant_id=auth_result["tenant_id"],
        api_key=auth_result["api_key"],
        is_super_admin=auth_result.get("is_super_admin", False),
    )


async def require_super_admin(ctx=Depends(get_tenant_context)):
    if not ctx.is_super_admin:
        raise HTTPException(status_code=403, detail="Super admin required")
    return ctx


async def get_db_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """Dependency for async DB session — fetched from app.state."""
    session_factory = getattr(request.app.state, "async_session_factory", None)
    if session_factory is None:
        raise RuntimeError(
            "Database not configured. "
            "Ensure async_session_factory is set in app.state during lifespan startup."
        )
    async with get_async_db(session_factory) as session:
        yield session


DbSession = Annotated[AsyncSession, Depends(get_db_session)]
