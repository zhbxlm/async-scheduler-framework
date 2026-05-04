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
    """


services = _DefaultServices()


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
