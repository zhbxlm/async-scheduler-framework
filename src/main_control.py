"""Control-plane worker entry point.

Runs long-lived background loops outside of task-api / ops-api:
- CronScheduler
- TaskReconciler
- CompensationService
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

from src.api.app_runtime import register_control_plane_resources, start_container_lifecycle, stop_container_lifecycle
from src.common.logging_config import configure_logging
from src.common.tracing import setup_tracing, shutdown_tracing

configure_logging()
setup_tracing(service_name="scheduler-control-plane", service_version="1.0.0")


@asynccontextmanager
async def _lifespan() -> AsyncIterator[None]:
    from config.settings_pydantic import settings
    from src.platform.container import ServiceContainer, set_container
    from src.common.http_client import init_http_client, close_http_client
    from src.common.async_db import async_dispose_engine

    init_http_client()
    container = await ServiceContainer.build_control_plane(settings)
    set_container(container)

    register_control_plane_resources(container, settings)
    await start_container_lifecycle(container)

    try:
        yield
    finally:
        await stop_container_lifecycle(container, drain_timeout=0.0)
        if container.async_engine:
            await async_dispose_engine(container.async_engine)
        await close_http_client()
        shutdown_tracing()


async def run_forever() -> None:
    async with _lifespan():
        while True:
            await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(run_forever())
