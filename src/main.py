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


# ── Stats endpoints (use app.state services) ──────────────────────────────

@app.get("/callbacks/stats")
async def callbacks_stats():
    dispatcher = getattr(services, "callback_dispatcher", None)
    if dispatcher and hasattr(dispatcher, "get_stats"):
        raw = await _call(dispatcher, "get_stats")
        retry_events = raw.get("recent_retry_events", [])
        dead_letters = raw.get("recent_dead_letters", [])
        raw["control_plane_summary"] = {
            "queue_depth_total": raw.get("retry_queue_size", 0) + raw.get("dead_letter_size", 0),
            "retry_queue_size": raw.get("retry_queue_size", 0),
            "dead_letter_size": raw.get("dead_letter_size", 0),
            "recent_retry_count": len(retry_events),
            "recent_dead_letter_count": len(dead_letters),
        }
        return raw
    return {
        "redis_backed": False,
        "retry_queue_size": 0,
        "dead_letter_size": 0,
        "max_inline_attempts": 3,
        "control_plane_summary": {
            "queue_depth_total": 0,
            "retry_queue_size": 0,
            "dead_letter_size": 0,
            "recent_retry_count": 0,
            "recent_dead_letter_count": 0,
        },
    }


@app.get("/async-proxy/stats")
async def async_proxy_stats():
    proxy = getattr(services, "async_proxy_sidecar", None) or getattr(services, "async_proxy", None)
    dispatcher = getattr(services, "callback_dispatcher", None)

    result: dict = {
        "running": False,
        "inflight": 0,
        "total_submitted": 0,
        "event_summary": {},
        "callback_control_plane": {},
        "recent_retry_items": [],
        "recent_dlq_items": [],
        "completion_kinds": {},
    }

    if proxy and hasattr(proxy, "get_stats"):
        proxy_stats = await _call(proxy, "get_stats")
        result.update(proxy_stats)
        status_counts: dict = proxy_stats.get("status_counts", {})
        result["event_summary"] = {
            "published_total": proxy_stats.get("published_total", 0),
            "terminal_total": status_counts.get("success", 0) + status_counts.get("failed", 0),
            "callback_event_total": status_counts.get("callback_requeued", 0),
            "running_total": status_counts.get("running", 0),
        }

    if dispatcher and hasattr(dispatcher, "get_stats"):
        cb = await _call(dispatcher, "get_stats")
        result["callback_control_plane"] = cb
        if isinstance(result.get("event_summary"), dict):
            result["event_summary"]["callback_control_plane"] = cb

    return result


@app.get("/quota/stats")
async def quota_stats():
    qm = getattr(services, "quota_manager", None)
    enforcer = getattr(services, "quota_enforcer", None)

    if qm and hasattr(qm, "stats"):
        raw: dict = await _call(qm, "stats")
        tenants = {
            tid: {
                "queued":      usage.get("task_count", usage.get("queued", 0)),
                "running":     usage.get("running_count", usage.get("running", 0)),
                "gpu":         usage.get("gpu_count", usage.get("gpu", 0)),
                "actors":      usage.get("actor_count", usage.get("actors", 0)),
                "max_queued":  usage.get("max_queued", 0),
                "max_running": usage.get("max_running", 0),
                "max_gpu":     usage.get("max_gpu", 0),
                "max_actor":   usage.get("max_actor", 0),
            }
            for tid, usage in raw.items()
        }
        return {"tenants": tenants, "summary": {"tenant_count": len(tenants)}}

    if enforcer and hasattr(enforcer, "get_usage"):
        usage = enforcer.get_usage("default")
        return {"tenants": {"default": usage}, "summary": {"tenant_count": 1}}

    return {"tenants": {}, "summary": {"tenant_count": 0}}


@app.get("/resources/stats")
async def resources_stats():
    rm = getattr(services, "resource_manager", None)
    if rm and hasattr(rm, "get_stats"):
        return rm.get_stats()
    return {
        "running": False,
        "tracked_capabilities": [],
        "scale_events_total": 0,
        "recent_events": [],
        "workload_summary": {
            "pending": 0, "running": 0, "scheduled": 0, "capabilities": {},
        },
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main:app", host="0.0.0.0", port=8000, reload=False)


# ── Legacy test-seam: tests patch `mod.services` to inject mocks ──────────
# This proxy object re-routes attribute access to the container when set,
# but can be replaced wholesale (mod.services = mock) by test code.
class _ServicesProxy:
    """Backward-compatible service accessor.

    Tests that do ``mod.services = FakeServices()`` still work because
    the stats endpoints read from this object.  Production code should
    prefer get_container() or request.app.state.*
    """
    def __getattr__(self, name: str):
        try:
            from src.common.container import get_container
            return getattr(get_container(), name, None)
        except RuntimeError:
            return None


services = _ServicesProxy()
