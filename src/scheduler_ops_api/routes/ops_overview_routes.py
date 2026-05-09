from __future__ import annotations

import time

from fastapi import Depends, HTTPException, Request

from scheduler_ops_api.auth import authenticate
from scheduler_ops_api.routes.ops_shared import logger, router
from src.platform import queue_keys as qk


@router.get("/health", summary="Service health check", include_in_schema=True)
async def ops_health(request: Request) -> dict:
    redis = getattr(request.app.state, "redis", None)
    redis_ok = False
    if redis:
        try:
            await redis.ping()
            redis_ok = True
        except Exception:
            pass
    return {"status": "ok" if redis_ok else "degraded", "redis": "ok" if redis_ok else "unavailable"}


@router.get("/overview", summary="System overview with capability stats", include_in_schema=True)
async def ops_overview(request: Request, _auth: dict = Depends(authenticate)) -> dict:
    redis = getattr(request.app.state, "redis", None)
    qm = getattr(request.app.state, "queue_manager", None)
    redis_ok = False
    if redis:
        try:
            await redis.ping()
            redis_ok = True
        except Exception:
            pass
    tenant_id = _auth.get("tenant_id", "default")
    is_super_admin = _auth.get("is_super_admin", False)
    caps = []
    if qm is not None:
        all_caps = await qm.discover_queue_capabilities()
        if is_super_admin:
            caps = all_caps
        else:
            cap_reg = getattr(request.app.state, "capability_registry", None)
            if cap_reg is not None:
                try:
                    tenant_caps = await cap_reg.list(tenant_id)
                    tenant_cap_ids = {c.get("capability_id", c) if isinstance(c, dict) else c for c in tenant_caps}
                    caps = [c for c in all_caps if c in tenant_cap_ids]
                except Exception:
                    caps = all_caps
                    logger.warning("capability_registry.list failed for tenant %s, showing all", tenant_id)
            else:
                caps = all_caps
    circuit_states: dict[str, str] = {}
    if redis is not None and caps:
        try:
            pipe = redis.pipeline()
            stats_keys = []
            for cap in caps:
                sk = qk.stats(cap)
                stats_keys.append((cap, sk))
                pipe.hget(sk, "circuit_state")
            raw_states = await pipe.execute()
            for (cap, _sk), raw in zip(stats_keys, raw_states):
                if raw:
                    circuit_states[cap] = raw.decode() if isinstance(raw, bytes) else raw
        except Exception:
            pass
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
                circuit_state = circuit_states.get(cap, "closed")
                if circuit_state == "open":
                    any_open = True
                utilization = round(running / max_concurrent, 2) if max_concurrent > 0 else 0.0
                cap_stats = {"pending": pending, "running": running, "max_concurrent": max_concurrent, "circuit_state": circuit_state, "utilization": utilization}
                total_pending += pending
                total_running += running
            except Exception as exc:
                cap_stats = {"error": str(exc)}
        capabilities_stats[cap] = cap_stats
    if not redis_ok:
        overall_status = "critical"
    elif any_open:
        overall_status = "degraded"
    else:
        overall_status = "ok"
    return {"status": overall_status, "redis": "ok" if redis_ok else "unavailable", "capabilities": capabilities_stats, "total_capabilities": len(caps), "total_pending": total_pending, "total_running": total_running, "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


@router.get("/stats", summary="Queue stats across all capabilities")
async def ops_stats(request: Request, _auth: dict = Depends(authenticate)) -> dict:
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
