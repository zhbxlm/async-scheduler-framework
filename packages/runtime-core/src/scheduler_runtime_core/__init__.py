"""async-scheduler-runtime-core: shared runtime implementation layer.

Public API:
    control_plane  — lifespan / run_forever / configure
    lifecycle      — ManagedResource, LifecycleManager, *Resource adapters
    lifecycle_helpers — start/stop/register_control_plane_resources
    settings       — AppSettings, settings singleton
    logging_config — configure_logging
    tracing        — setup_tracing, shutdown_tracing, trace_span
    http_client    — init/close/get_http_client
    async_db       — init_async_engine, async_dispose_engine, get_async_db
"""
from __future__ import annotations

from scheduler_runtime_core import (  # noqa: F401 — re-export for convenience
    app_factory,
    async_db,
    control_plane,
    http_client,
    lifecycle,
    lifecycle_helpers,
    logging_config,
    settings,
    tracing,
)
