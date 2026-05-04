"""ops routes — /ops/v1/ops  (operational control)
aligned with docs/deepwiki-reference/API 参考.md
"""
from __future__ import annotations

import time
from fastapi import APIRouter, Depends, HTTPException, Request

from src.api.auth import authenticate

router = APIRouter(prefix="/ops/v1", tags=["ops"])


@router.get("/health", summary="Service health check", include_in_schema=True)
async def ops_health(request: Request) -> dict:
    """Return health of Redis, DB and registered components."""
    redis = getattr(request.app.state, "redis", None)
    redis_ok = False
    if redis:
        try:
            await redis.ping()
            redis_ok = True
        except Exception:
            pass

    return {
        "status": "ok" if redis_ok else "degraded",
        "redis": "ok" if redis_ok else "unavailable",
    }


@router.get("/overview", summary="System overview with capability stats", include_in_schema=True)
async def ops_overview(request: Request) -> dict:
    """Return system-wide health summary with per-capability statistics."""
    redis = getattr(request.app.state, "redis", None)
    qm = getattr(request.app.state, "queue_manager", None)

    redis_ok = False
    if redis:
        try:
            await redis.ping()
            redis_ok = True
        except Exception:
            pass

    # Get capabilities
    caps = []
    if qm is not None:
        caps = await qm.discover_queue_capabilities()

    capabilities_stats = {}
    total_pending = 0
    total_running = 0
    any_open = False

    for cap in caps:
        cap_stats = {}
        if qm is not None:
            try:
                snapshot = await qm.get_queue_snapshot(cap)
                pending = snapshot.get("pending", 0)
                running = snapshot.get("running", 0)
                max_concurrent = snapshot.get("max_concurrent", 8)
                circuit_state = "closed"

                # Try to get circuit_state from redis stats hash
                if redis is not None:
                    try:
                        stats_key = f"queue:{cap}:stats"
                        circuit_raw = await redis.hget(stats_key, "circuit_state")
                        if circuit_raw:
                            circuit_state = circuit_raw.decode() if isinstance(circuit_raw, bytes) else circuit_raw
                            if circuit_state == "open":
                                any_open = True
                    except Exception:
                        pass

                utilization = round(running / max_concurrent, 2) if max_concurrent > 0 else 0.0

                cap_stats = {
                    "pending": pending,
                    "running": running,
                    "max_concurrent": max_concurrent,
                    "circuit_state": circuit_state,
                    "utilization": utilization,
                }
                total_pending += pending
                total_running += running
            except Exception as exc:
                cap_stats = {"error": str(exc)}

        capabilities_stats[cap] = cap_stats

    # Determine overall status
    if not redis_ok:
        overall_status = "critical"
    elif any_open:
        overall_status = "degraded"
    else:
        overall_status = "ok"

    return {
        "status": overall_status,
        "redis": "ok" if redis_ok else "unavailable",
        "capabilities": capabilities_stats,
        "total_capabilities": len(caps),
        "total_pending": total_pending,
        "total_running": total_running,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


@router.get("/stats", summary="Queue stats across all capabilities")
async def ops_stats(
    request: Request,
    _auth: dict = Depends(authenticate),
) -> dict:
    """Return queue depths for all registered capabilities."""
    qm = getattr(request.app.state, "queue_manager", None)
    if qm is None:
        raise HTTPException(status_code=503, detail="QueueManager not initialised")
    caps = await qm.discover_queue_capabilities()
    stats = {}
    for cap in caps:
        try:
            stats[cap] = await qm.get_queue_snapshot(cap)
        except Exception as exc:
            stats[cap] = {"error": str(exc)}
    return {"capabilities": stats, "total": len(caps)}


@router.post("/reconcile", summary="Manually trigger task reconciliation")
async def trigger_reconcile(
    request: Request,
    _auth: dict = Depends(authenticate),
) -> dict:
    """Trigger one reconcile cycle (all three phases)."""
    reconciler = getattr(request.app.state, "task_reconciler", None)
    if reconciler is None:
        raise HTTPException(status_code=503, detail="TaskReconciler not initialised")
    batch = await reconciler._scan_task_batch()
    import asyncio
    await asyncio.gather(
        reconciler._phase1_double_write(batch),
        reconciler._phase2_stuck_recovery(batch),
        reconciler._phase3_lost_callback(batch),
        return_exceptions=True,
    )
    return {"triggered": True, "batch_size": len(batch)}


@router.post("/callbacks/process", summary="Process due callback retries")
async def process_callbacks(
    request: Request,
    batch_size: int = 50,
    _auth: dict = Depends(authenticate),
) -> dict:
    """Drain due entries from the callback retry ZSET."""
    tcn = getattr(request.app.state, "task_completion_node", None)
    if tcn is None:
        raise HTTPException(status_code=503, detail="TaskCompletionNode not initialised")
    processed = await tcn.process_due_callbacks(batch_size=batch_size)
    return {"processed": processed}


@router.get("/queue/{capability}/snapshot", summary="Queue snapshot for one capability")
async def queue_snapshot(
    capability: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> dict:
    qm = getattr(request.app.state, "queue_manager", None)
    if qm is None:
        raise HTTPException(status_code=503, detail="QueueManager not initialised")
    return await qm.get_queue_snapshot(capability)


@router.post("/queue/{capability}/cleanup-stale", summary="Cleanup stale running entries")
async def cleanup_stale(
    capability: str,
    request: Request,
    max_age_seconds: float = 300.0,
    _auth: dict = Depends(authenticate),
) -> dict:
    qm = getattr(request.app.state, "queue_manager", None)
    if qm is None:
        raise HTTPException(status_code=503, detail="QueueManager not initialised")
    removed_tasks = await qm.cleanup_stale_running(capability, max_age_seconds)
    removed_slots = await qm.cleanup_stale_slots(capability, max_age_seconds)
    return {"capability": capability, "removed_tasks": removed_tasks, "removed_slots": removed_slots}
