"""Package-owned ops-api lifespan management."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from scheduler_ops_api.container import build_container
from scheduler_ops_api.runtime import mount_container_state, start_container_lifecycle, stop_container_lifecycle
from scheduler_runtime_core.http_client import close_http_client, init_http_client
from scheduler_runtime_core.tracing import shutdown_tracing


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    from config.settings_pydantic import settings

    init_http_client()
    container = await build_container(settings)

    mount_container_state(
        app,
        container,
        (
            "redis_client",
            "capability_registry",
            "cluster_registry",
            "node_registry",
            "schedule_registry",
            "tenant_registry",
            "dag_loader",
            "queue_manager",
        ),
    )
    app.state.redis = container.redis_client

    await start_container_lifecycle(container)
    try:
        yield
    finally:
        await stop_container_lifecycle(container)
        await close_http_client()
        shutdown_tracing()
