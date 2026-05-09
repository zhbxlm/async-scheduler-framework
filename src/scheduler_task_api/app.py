"""scheduler_task_api.app — canonical FastAPI application factory for task-api."""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI

from scheduler_runtime_core.logging_config import configure_logging
from scheduler_runtime_core.tracing import instrument_fastapi, setup_tracing
from scheduler_task_api.lifespan import lifespan
from scheduler_task_api.routes.health import router as health_router
from scheduler_task_api.routes.metrics import router as metrics_router
from scheduler_task_api.routes.tasks import router as tasks_router

from scheduler_task_api.middleware import RequestIDMiddleware
from scheduler_task_api.error_handling import (
    BusinessError,
    ExternalServiceError,
    SystemError,
    handle_business_error,
    handle_external_service_error,
    handle_system_error,
)
from scheduler_task_api.middleware import MetricsMiddleware

configure_logging()
setup_tracing(service_name="scheduler-task-api", service_version="1.0.0")


def create_app(**kwargs: Any) -> FastAPI:
    app = FastAPI(
        title="Ray Async Task API",
        version="1.0.0",
        description="Task submission and query API. Requires MySQL + Redis.",
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

    app.include_router(tasks_router, prefix="/api/v1")
    app.include_router(health_router, prefix="/api/v1")
    app.include_router(metrics_router)

    @app.get("/health", tags=["health"])
    async def health() -> dict[str, str]:
        return {"status": "ok", "note": "Use /health/ for detailed health checks"}

    return app
