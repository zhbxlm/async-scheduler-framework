"""Shared control-plane runtime implementation.

This module is the shared implementation target for independently packaged
control-plane roles and monorepo entrypoints.

container_factory: async callable(settings) -> container — injected at runtime
to decouple runtime-core from the monorepo src.platform.container.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, Awaitable

from scheduler_runtime_core.lifecycle_helpers import (
    register_control_plane_resources,
    start_container_lifecycle,
    stop_container_lifecycle,
)
from scheduler_runtime_core.logging_config import configure_logging
from scheduler_runtime_core.tracing import setup_tracing, shutdown_tracing

configure_logging()
setup_tracing(service_name="scheduler-control-plane", service_version="1.0.0")

# Injected by caller (monorepo main_control or packaged entrypoint)
_container_factory: Callable[..., Awaitable[Any]] | None = None
_set_container_fn: Callable[[Any], None] | None = None


def configure(
    container_factory: Callable[..., Awaitable[Any]],
    set_container: Callable[[Any], None],
) -> None:
    """Inject runtime dependencies.

    Args:
        container_factory: ``async (settings) -> container`` builder.
        set_container: global container setter from the platform layer.
    """
    global _container_factory, _set_container_fn
    _container_factory = container_factory
    _set_container_fn = set_container


def _default_factory():
    """Fall back to monorepo factory when configure() was not called."""
    from src.platform.container import ServiceContainer, set_container  # monorepo-wiring: fallback
    return ServiceContainer.build_control_plane, set_container


@asynccontextmanager
async def lifespan() -> AsyncIterator[None]:
    from scheduler_runtime_core.settings import settings
    from scheduler_runtime_core.http_client import init_http_client, close_http_client
    from scheduler_runtime_core.async_db import async_dispose_engine

    if _container_factory is not None:
        build_fn = _container_factory
        set_fn = _set_container_fn
    else:
        build_fn, set_fn = _default_factory()

    init_http_client()
    container = await build_fn(settings)
    if set_fn is not None:
        set_fn(container)

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


def get_run_forever():
    return run_forever
