"""Main API server entry point."""
from __future__ import annotations

import asyncio
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

# Structured logging (JSON in production, plain text in dev)
configure_logging()

# Initialise tracing before creating the app (spans start from here)
setup_tracing(service_name="scheduler-api", service_version="1.0.0")


# ── Lifespan ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage application startup and shutdown."""
    # ── startup ──────────────────────────────────────────────────
    from config.settings_compat import settings
    from src.common.container import ServiceContainer, set_container
    from src.common.lifecycle import (
        get_lifecycle_manager,
        TaskReconcilerResource,
        CronSchedulerResource,
    )

    # init shared http client pool
    from src.common.http_client import init_http_client
    init_http_client()

    container = await ServiceContainer.build(settings)
    set_container(container)

    # Mount onto app.state so route helpers can access via request.app.state
    app.state.redis = container.redis_client
    app.state.capability_registry = container.capability_registry
    app.state.cluster_registry = container.cluster_registry
    app.state.node_registry = container.node_registry
    app.state.schedule_registry = container.schedule_registry
    app.state.tenant_registry = container.tenant_registry
    app.state.dag_loader = container.dag_loader
    app.state.task_creator = container.task_creator
    app.state.task_reconciler = container.task_reconciler
    app.state.task_completion_node = container.task_completion_node
    app.state.queue_manager = container.queue_manager

    manager = get_lifecycle_manager()
    if settings.background.reconcile.enabled and container.task_reconciler:
        manager.register_resource(TaskReconcilerResource(container.task_reconciler))
    if settings.background.cron.enabled and container.cron_scheduler:
        manager.register_resource(CronSchedulerResource(container.cron_scheduler))
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
    title="Ray Async API",
    version="1.0.0",
    description="Async task scheduling framework powered by Ray.",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

instrument_fastapi(app)
app.add_middleware(RequestIDMiddleware)

app.add_exception_handler(SystemError, handle_system_error)
app.add_exception_handler(BusinessError, handle_business_error)
app.add_exception_handler(ExternalServiceError, handle_external_service_error)


# ── Routes ────────────────────────────────────────────────────────────────

from src.api.routes.tasks import router as tasks_router
from src.api.routes.capabilities import router as capabilities_router
from src.api.routes.clusters import router as clusters_router
from src.api.routes.dags import router as dags_router
from src.api.routes.nodes import router as nodes_router
from src.api.routes.ops import router as ops_router
from src.api.routes.schedules import router as schedules_router
from src.api.routes.tenants import router as tenants_router
from src.api.routes.health import router as health_router

for _r in (
    tasks_router,
    capabilities_router,
    clusters_router,
    dags_router,
    nodes_router,
    ops_router,
    schedules_router,
    tenants_router,
    health_router,
):
    app.include_router(_r)


# ── Helpers ───────────────────────────────────────────────────────────────

def _get_service(app_state, name: str):
    """Safely fetch a service from app.state; return None if missing."""
    return getattr(app_state, name, None)


async def _call(obj, method: str):
    """Call obj.method(), awaiting if it returns a coroutine."""
    result = getattr(obj, method)()
    if asyncio.iscoroutine(result):
        result = await result
    return result


# ── Legacy health endpoint ────────────────────────────────────────────────

@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok", "note": "Use /health/ for detailed health checks"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main:app", host="0.0.0.0", port=8000, reload=False)
