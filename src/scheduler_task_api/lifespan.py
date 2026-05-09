"""Package-owned task-api lifespan management."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from scheduler_task_api.container import build_container
from scheduler_task_api.runtime import (
    cache_auth_settings,
    mount_container_state,
    start_container_lifecycle,
    stop_container_lifecycle,
)
from scheduler_task_api.async_db import async_dispose_engine
from scheduler_runtime_core.http_client import close_http_client, init_http_client
from scheduler_runtime_core.tracing import shutdown_tracing


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    from config.settings_pydantic import settings
    from scheduler_task_api.error_handling import BusinessError

    if not settings.mysql.url:
        raise BusinessError(
            "task‑api requires MySQL; set MYSQL_URL environment variable",
            error_code="CONFIG_MYSQL_MISSING",
        )

    init_http_client()
    container = await build_container(settings)

    mount_container_state(
        app,
        container,
        (
            "redis_client",
            "tenant_registry",
            "schedule_registry",
            "queue_manager",
            "task_creator",
            "task_completion_node",
            "async_engine",
            "async_session_factory",
        ),
    )
    app.state.redis = container.redis_client
    cache_auth_settings(app, settings)

    await start_container_lifecycle(container)
    try:
        yield
    finally:
        await stop_container_lifecycle(container)
        if app.state.async_engine:
            await async_dispose_engine(app.state.async_engine)
        await close_http_client()
        shutdown_tracing()
