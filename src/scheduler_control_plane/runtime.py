"""Package-owned control-plane runtime wiring."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

from scheduler_control_plane.container import build_container, set_container
from scheduler_runtime_core.async_db import async_dispose_engine
from scheduler_runtime_core.http_client import close_http_client, init_http_client
from scheduler_runtime_core.lifecycle_helpers import (
    register_control_plane_resources,
    start_container_lifecycle,
    stop_container_lifecycle,
)
from scheduler_runtime_core.logging_config import configure_logging
from scheduler_runtime_core.tracing import setup_tracing, shutdown_tracing

configure_logging()
setup_tracing(service_name="scheduler-control-plane", service_version="1.0.0")


@asynccontextmanager
async def lifespan() -> AsyncIterator[None]:
    from scheduler_runtime_core.settings import settings

    init_http_client()
    container = await build_container(settings)
    set_container(container)

    register_control_plane_resources(container, settings)
    await start_container_lifecycle(container)
    try:
        yield
    finally:
        await stop_container_lifecycle(container, drain_timeout=0.0)
        if getattr(container, "async_engine", None):
            await async_dispose_engine(container.async_engine)
        await close_http_client()
        shutdown_tracing()


async def run_forever() -> None:
    async with lifespan():
        while True:
            await asyncio.sleep(3600)
