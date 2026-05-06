"""Ops API server entry point (port 8000).

Handles operational management, Redis-only registries and CronScheduler.
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
# MetricsMiddleware handles request tracking
from src.middleware.metrics_middleware import MetricsMiddleware

# Structured logging (JSON in production, plain text in dev)
configure_logging()

# Initialise tracing before creating the app (spans start from here)
setup_tracing(service_name="scheduler-ops-api", service_version="1.0.0")


# ── Lifespan ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage application startup and shutdown."""
    # ── startup ──────────────────────────────────────────────────
    from config.settings_pydantic import settings
    from src.common.container import ServiceContainer, set_container
    from src.common.lifecycle import get_lifecycle_manager

    # init shared http client pool
    from src.common.http_client import init_http_client
    init_http_client()

    container = await ServiceContainer.build_ops_api(settings)
    set_container(container)

    # Mount onto app.state so route helpers can access via request.app.state
    app.state.redis = container.redis_client
    app.state.capability_registry = container.capability_registry
    app.state.cluster_registry = container.cluster_registry
    app.state.node_registry = container.node_registry
    app.state.schedule_registry = container.schedule_registry
    app.state.tenant_registry = container.tenant_registry
    app.state.dag_loader = container.dag_loader
    app.state.queue_manager = container.queue_manager

    manager = get_lifecycle_manager()
    await manager.start_all()

    yield

    # ── shutdown ──────────────────────────────────────────────────
    manager = get_lifecycle_manager()
    await manager.stop_all()
    from src.common.http_client import close_http_client
    await close_http_client()
    shutdown_tracing()


# ── App ───────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Ray Async Ops API",
    version="1.0.0",
    description="Operational management API - Redis-only registries, CronScheduler.",
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

from src.api.routes.capabilities import router as capabilities_router
from src.api.routes.clusters import router as clusters_router
from src.api.routes.dags import router as dags_router
from src.api.routes.nodes import router as nodes_router
from src.api.routes.ops import router as ops_router
from src.api.routes.schedules import router as schedules_router
from src.api.routes.tenants import router as tenants_router
from src.api.routes.health import router as health_router
from src.api.routes.metrics import router as metrics_router
from src.api.routes.alerts import router as alerts_router

for _r in (
    capabilities_router,
    clusters_router,
    dags_router,
    nodes_router,
    ops_router,
    schedules_router,
    tenants_router,
    metrics_router,
    alerts_router,
):
    app.include_router(_r)

# Health gets /api/v1 prefix for consistency with task-api
app.include_router(health_router, prefix="/api/v1")


# ── Legacy health endpoint ────────────────────────────────────────────────

@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok", "note": "Use /health/ for detailed health checks"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main:app", host="0.0.0.0", port=8000, reload=False)
