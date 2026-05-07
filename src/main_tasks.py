"""Task API server entry point (port 8001).

Handles task submission, query, result retrieval and cancellation.
Depends on MySQL (TaskRecord ORM) + Redis.
Long-running background loops are hosted by the control-plane worker.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from src.common.error_handling import (
    SystemError,
    BusinessError,
    ExternalServiceError,
    handle_system_error,
    handle_business_error,
    handle_external_service_error,
)
from src.common.tracing import setup_tracing, instrument_fastapi, shutdown_tracing
from src.common.logging_config import configure_logging
from src.api.middleware import RequestIDMiddleware
from src.middleware.metrics_middleware import MetricsMiddleware

configure_logging()
setup_tracing(service_name="scheduler-task-api", service_version="1.0.0")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage application startup and shutdown."""
    # ── startup ──────────────────────────────────────────────────
    from config.settings_pydantic import settings
    from src.platform.container import ServiceContainer, set_container
    from src.common.lifecycle import get_lifecycle_manager
    from src.common.error_handling import BusinessError

    # Fail‑fast: MySQL must be configured for task‑api
    if not settings.mysql.url:
        raise BusinessError(
            "task‑api requires MySQL; set MYSQL_URL environment variable",
            error_code="CONFIG_MYSQL_MISSING",
        )

    # init shared http client pool
    from src.common.http_client import init_http_client
    init_http_client()

    from src.common.async_db import async_dispose_engine

    container = await ServiceContainer.build_task_api(settings)
    set_container(container)

    # Mount onto app.state so route helpers can access via request.app.state
    app.state.redis = container.redis_client
    app.state.tenant_registry = container.tenant_registry
    app.state.schedule_registry = container.schedule_registry
    app.state.queue_manager = container.queue_manager
    app.state.task_creator = container.task_creator
    app.state.task_reconciler = container.task_reconciler
    app.state.task_completion_node = container.task_completion_node
    
    # Database engine + session factory (no module-level globals)
    app.state.async_engine = container.async_engine
    app.state.async_session_factory = container.async_session_factory
    
    # Cache auth settings for authenticate() - avoids repeated module imports
    app.state.auth_settings = {
        'super_admin_key': settings.tenant.super_admin_api_key,
        'multi_tenant_enabled': settings.tenant.multi_tenant_enabled,
        'tenant_id_header': settings.tenant.tenant_id_header,
    }

    manager = get_lifecycle_manager()
    await manager.start_all()

    yield

    # ── shutdown ──────────────────────────────────────────────────
    manager = get_lifecycle_manager()
    await manager.stop_all()
    if app.state.async_engine:
        await async_dispose_engine(app.state.async_engine)
    from src.common.http_client import close_http_client
    await close_http_client()
    shutdown_tracing()


# ── App ───────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Ray Async Task API",
    version="1.0.0",
    description="Task submission and query API. Requires MySQL + Redis.",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

instrument_fastapi(app)
app.add_middleware(MetricsMiddleware)
app.add_middleware(RequestIDMiddleware)

app.add_exception_handler(SystemError, handle_system_error)
app.add_exception_handler(BusinessError, handle_business_error)
app.add_exception_handler(ExternalServiceError, handle_external_service_error)


# ── Routes ────────────────────────────────────────────────────────────────

from src.api.routes.tasks import router as tasks_router
from src.api.routes.health import router as health_router
from src.api.routes.metrics import router as metrics_router

app.include_router(tasks_router, prefix="/api/v1")
app.include_router(health_router, prefix="/api/v1")
app.include_router(metrics_router)


# ── Legacy health endpoint ────────────────────────────────────────────────

@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok", "note": "Use /health/ for detailed health checks"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main_tasks:app", host="0.0.0.0", port=8001, reload=False)