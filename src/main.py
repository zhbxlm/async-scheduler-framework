"""Main API server entry point — aligned with deepwiki-reference/项目概述.md"""
from __future__ import annotations
import asyncio
from fastapi import FastAPI

app = FastAPI(title="Ray Async API", version="0.1.0")

# ── Register ops/task routes ───────────────────────────────────────────────
from src.api.routes.tasks import router as tasks_router
from src.api.routes.capabilities import router as capabilities_router
from src.api.routes.clusters import router as clusters_router
from src.api.routes.dags import router as dags_router
from src.api.routes.nodes import router as nodes_router
from src.api.routes.ops import router as ops_router
from src.api.routes.schedules import router as schedules_router
from src.api.routes.tenants import router as tenants_router

for _r in (
    tasks_router,
    capabilities_router,
    clusters_router,
    dags_router,
    nodes_router,
    ops_router,
    schedules_router,
    tenants_router,
):
    app.include_router(_r)


# ── Service container (replaceable for tests) ─────────────────────────────
class _DefaultServices:
    """Stub service container. Replace attributes for testing.
    Supported: callback_dispatcher, async_proxy_sidecar, async_proxy,
               quota_manager, quota_enforcer, resource_manager
               capability_registry, cluster_registry, node_registry,
               schedule_registry, tenant_registry, dag_loader,
               task_creator, task_reconciler, cron_scheduler, queue_manager
    """
    capability_registry = None
    cluster_registry = None
    node_registry = None
    schedule_registry = None
    tenant_registry = None
    dag_loader = None
    task_creator = None
    task_reconciler = None
    cron_scheduler = None
    queue_manager = None


services = _DefaultServices()


async def init_services() -> None:
    """Initialize platform services and registries."""
    from src.platform.capability_registry import CapabilityRegistry
    from src.platform.cluster_registry import ClusterRegistry
    from src.platform.node_registry import NodeRegistry
    from src.platform.schedule_registry import ScheduleRegistry
    from src.platform.tenant_registry import TenantRegistry
    from src.platform.dag_loader import DagLoader
    from src.platform.task_reconciler import TaskReconciler
    from src.platform.cron_scheduler import CronScheduler
    from src.platform.queue_manager import QueueManager
    from src.platform.task_creator import TaskCreator
    from src.common.redis_client import create_redis_client
    from src.common.db import init_engine, get_db
    from config.settings import settings

    redis_client = create_redis_client(settings.redis.url or None)
    # DB engine (optional)
    if settings.infra.mysql.url:
        init_engine(settings.infra.mysql.url)

    services.capability_registry = CapabilityRegistry(redis_client)
    services.cluster_registry = ClusterRegistry(redis_client)
    services.node_registry = NodeRegistry(redis_client)
    services.schedule_registry = ScheduleRegistry(redis_client)
    services.tenant_registry = TenantRegistry(redis_client)
    services.dag_loader = DagLoader(redis_client=redis_client)  # uses default config/dags
    services.queue_manager = QueueManager(redis_client)
    services.task_creator = TaskCreator(
        redis_client=redis_client,
        queue_manager=services.queue_manager,
        db_session_factory=get_db if settings.infra.mysql.url else None,
    )
    services.task_reconciler = TaskReconciler(
        redis_client=redis_client,
        db_session_factory=get_db if settings.infra.mysql.url else None,
        queue_manager=services.queue_manager,
        interval_seconds=settings.background.reconcile.interval_seconds,
        stuck_max_per_tick=settings.background.reconcile.stuck_max_per_tick,
        stuck_task_max_age_seconds=settings.background.reconcile.stuck_task_max_age_seconds,
        batch_size=settings.background.reconcile.batch_size,
    )
    services.cron_scheduler = CronScheduler(
        redis_client=redis_client,
        schedule_repository=services.schedule_registry,
        task_creator=services.task_creator,
        poll_interval=settings.background.cron.check_interval,
    )
    # other services can be added here when needed


# ── Startup hook ──────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event() -> None:
    await init_services()
    # Mount registries onto app.state for compatibility with existing route helpers
    app.state.capability_registry = services.capability_registry
    app.state.cluster_registry = services.cluster_registry
    app.state.node_registry = services.node_registry
    app.state.schedule_registry = services.schedule_registry
    app.state.tenant_registry = services.tenant_registry
    app.state.dag_loader = services.dag_loader
    app.state.task_creator = services.task_creator
    app.state.task_reconciler = services.task_reconciler
    # Start background services if enabled
    if settings.background.reconcile.enabled and services.task_reconciler:
        await services.task_reconciler.start()
    if settings.background.cron.enabled and services.cron_scheduler:
        await services.cron_scheduler.start()


# ── Helpers ────────────────────────────────────────────────────────────────
async def _call(obj, method: str):
    result = getattr(obj, method)()
    if asyncio.iscoroutine(result):
        result = await result
    return result


# ── Health ─────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok"}


# ── /callbacks/stats ───────────────────────────────────────────────────────
@app.get("/callbacks/stats")
async def callbacks_stats():
    dispatcher = getattr(services, "callback_dispatcher", None)
    if dispatcher and hasattr(dispatcher, "get_stats"):
        raw = await _call(dispatcher, "get_stats")
        # Build derived control_plane_summary
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


# ── /async-proxy/stats ─────────────────────────────────────────────────────
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
        # Build event_summary from proxy stats
        status_counts: dict = proxy_stats.get("status_counts", {})
        result["event_summary"] = {
            "published_total":    proxy_stats.get("published_total", 0),
            "terminal_total":     status_counts.get("success", 0) + status_counts.get("failed", 0),
            "callback_event_total": status_counts.get("callback_requeued", 0),
            "running_total":      status_counts.get("running", 0),
        }

    if dispatcher and hasattr(dispatcher, "get_stats"):
        cb = await _call(dispatcher, "get_stats")
        # Embed at both top-level and inside event_summary
        result["callback_control_plane"] = cb
        if isinstance(result.get("event_summary"), dict):
            result["event_summary"]["callback_control_plane"] = cb

    return result


# ── /quota/stats ───────────────────────────────────────────────────────────
@app.get("/quota/stats")
async def quota_stats():
    qm = getattr(services, "quota_manager", None)
    if qm and hasattr(qm, "stats"):
        raw: dict = await _call(qm, "stats")
        # Normalize field names for API consumers
        tenants = {}
        for tid, usage in raw.items():
            tenants[tid] = {
                "queued":      usage.get("task_count", usage.get("queued", 0)),
                "running":     usage.get("running_count", usage.get("running", 0)),
                "gpu":         usage.get("gpu_count", usage.get("gpu", 0)),
                "actors":      usage.get("actor_count", usage.get("actors", 0)),
                "max_queued":  usage.get("max_queued", 0),
                "max_running": usage.get("max_running", 0),
                "max_gpu":     usage.get("max_gpu", 0),
                "max_actor":   usage.get("max_actor", 0),
            }
        return {"tenants": tenants, "summary": {"tenant_count": len(tenants)}}
    enforcer = getattr(services, "quota_enforcer", None)
    if enforcer and hasattr(enforcer, "get_usage"):
        usage = enforcer.get_usage("default")
        return {"tenants": {"default": usage}, "summary": {"tenant_count": 1}}
    return {"tenants": {}, "summary": {"tenant_count": 0}}


# ── /resources/stats ───────────────────────────────────────────────────────
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
