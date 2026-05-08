"""FastAPI dependencies — auth, tenant context, DB session, runtime resources."""
from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Annotated, Any

from fastapi import Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

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


def _get_app_state_attr(request: Request, attr_name: str, detail: str) -> Any:
    value = getattr(request.app.state, attr_name, None)
    if value is None:
        raise HTTPException(status_code=503, detail=detail)
    return value


def get_cluster_registry(request: Request):
    return _get_app_state_attr(request, "cluster_registry", "ClusterRegistry not initialised")


def get_node_registry(request: Request):
    return _get_app_state_attr(request, "node_registry", "NodeRegistry not initialised")


def get_schedule_registry(request: Request):
    return _get_app_state_attr(request, "schedule_registry", "ScheduleRegistry not initialised")


def get_dag_loader(request: Request):
    return _get_app_state_attr(request, "dag_loader", "DagLoader not initialised")


def get_redis(request: Request):
    return getattr(request.app.state, "redis", None)


ClusterRegistryDep = Annotated[Any, Depends(get_cluster_registry)]
NodeRegistryDep = Annotated[Any, Depends(get_node_registry)]
ScheduleRegistryDep = Annotated[Any, Depends(get_schedule_registry)]
DagLoaderDep = Annotated[Any, Depends(get_dag_loader)]
RedisDep = Annotated[Any | None, Depends(get_redis)]
