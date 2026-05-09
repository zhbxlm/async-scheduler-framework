"""scheduler_ops_api.app — canonical FastAPI application factory for ops-api."""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI

from scheduler_runtime_core.logging_config import configure_logging
from scheduler_runtime_core.tracing import instrument_fastapi, setup_tracing
from scheduler_ops_api.lifespan import lifespan
from scheduler_ops_api.routes.alerts import router as alerts_router
from scheduler_ops_api.routes.capabilities import router as capabilities_router
from scheduler_ops_api.routes.clusters import router as clusters_router
from scheduler_ops_api.routes.dags import router as dags_router
from scheduler_ops_api.routes.health import router as health_router
from scheduler_ops_api.routes.metrics import router as metrics_router
from scheduler_ops_api.routes.nodes import router as nodes_router
from scheduler_ops_api.routes.ops import router as ops_router
from scheduler_ops_api.routes.schedules import router as schedules_router
from scheduler_ops_api.routes.tenants import router as tenants_router

from scheduler_ops_api.middleware import RequestIDMiddleware
from scheduler_ops_api.error_handling import (
    BusinessError,
    ExternalServiceError,
    SystemError,
    handle_business_error,
    handle_external_service_error,
    handle_system_error,
)
from scheduler_ops_api.middleware import MetricsMiddleware

configure_logging()
setup_tracing(service_name="scheduler-ops-api", service_version="1.0.0")


def create_app(**kwargs: Any) -> FastAPI:
    app = FastAPI(
        title="Ray Async Ops API",
        version="1.0.0",
        description="Operational management API - Redis-only registries, CronScheduler.",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
        **kwargs,
    )

    instrument_fastapi(app)
    app.add_middleware(MetricsMiddleware)
    app.add_middleware(RequestIDMiddleware)

    app.add_exception_handler(SystemError, handle_system_error)
    app.add_exception_handler(BusinessError, handle_business_error)
    app.add_exception_handler(ExternalServiceError, handle_external_service_error)

    for router in (
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
        app.include_router(router)
    app.include_router(health_router, prefix="/api/v1")

    @app.get("/health", tags=["health"])
    async def health() -> dict[str, str]:
        return {"status": "ok", "note": "Use /health/ for detailed health checks"}

    return app
