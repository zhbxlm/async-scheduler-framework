"""ops routes — /ops/v1/ops  (operational control)
aligned with docs/deepwiki-reference/API 参考.md
"""
from __future__ import annotations

import time
from typing import Dict
from fastapi import APIRouter, Depends, HTTPException, Request

from src.api.auth import authenticate
from src.platform import queue_keys as qk

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
async def ops_overview(request: Request, _auth: dict = Depends(authenticate)) -> dict:
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

    # Batch-fetch queue snapshots (1 call per cap, but circuit_state needs Redis)
    # Use pipeline for circuit_state to avoid N+1
    circuit_states: Dict[str, str] = {}
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


@router.get("/tasks/{task_id}/debug", summary="Debug information for a specific task")
async def task_debug(
    task_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> dict:
    """Return comprehensive debug information for a task.
    
    Aggregates data from multiple sources:
    - MySQL task record (if DB available)
    - Redis queue status (pending/running/completed)
    - Log context (if logging system supports it)
    """
    redis = getattr(request.app.state, "redis", None)
    qm = getattr(request.app.state, "queue_manager", None)
    db_session = getattr(request.app.state, "async_session_factory", None)
    
    result = {"task_id": task_id, "sources": {}}
    
    # 1. Check MySQL task record
    if db_session:
        from src.models.task import TaskRecord
        async with db_session() as session:
            from sqlalchemy import select
            query = select(TaskRecord).where(TaskRecord.task_id == task_id)
            task_record = (await session.execute(query)).scalar_one_or_none()
            if task_record:
                result["sources"]["mysql"] = {
                    "task_id": task_record.task_id,
                    "status": task_record.status.value,
                    "created_at": task_record.created_at.isoformat() if task_record.created_at else None,
                    "updated_at": task_record.updated_at.isoformat() if task_record.updated_at else None,
                    "tenant_id": task_record.tenant_id,
                    "task_type": task_record.task_type,
                    "priority": task_record.priority.value if task_record.priority else None,
                }
            else:
                result["sources"]["mysql"] = {"found": False}
    else:
        result["sources"]["mysql"] = {"available": False}
    
    # 2. Check Redis queues
    if redis and qm:
        # Check all capabilities' pending/running queues
        capabilities = await redis.smembers("queue:capabilities:registry")
        redis_status = {"capabilities": [], "found_in": []}
        for cap in capabilities:
            pending_key = f"queue:{cap}:pending"
            running_key = f"queue:{cap}:running"
            completed_key = f"queue:{cap}:stats"
            
            # Check pending
            pending_score = await redis.zscore(pending_key, task_id)
            if pending_score is not None:
                rank = await redis.zrank(pending_key, task_id)
                redis_status["found_in"].append({
                    "queue": "pending",
                    "capability": cap,
                    "score": pending_score,
                    "position": rank + 1 if rank is not None else None,
                })
            
            # Check running
            running_score = await redis.zscore(running_key, task_id)
            if running_score is not None:
                redis_status["found_in"].append({
                    "queue": "running",
                    "capability": cap,
                    "score": running_score,
                })
            
            # Check completed stats
            completed_count = await redis.hget(completed_key, "total_completed")
            if completed_count:
                redis_status["capabilities"].append(cap)
        
        result["sources"]["redis"] = redis_status
    else:
        result["sources"]["redis"] = {"available": False}
    
    # 3. System recommendations
    recommendations = []
    mysql_found = result["sources"].get("mysql", {}).get("found", True) != False
    redis_found = result["sources"].get("redis", {}).get("found_in", [])
    
    if not mysql_found and not redis_found:
        recommendations.append("Task not found in MySQL or Redis. May have been cleaned up.")
    elif redis_found:
        for entry in redis_found:
            if entry["queue"] == "pending":
                recommendations.append(f"Task is pending in {entry['capability']} queue (position {entry.get('position', 'unknown')}).")
            elif entry["queue"] == "running":
                recommendations.append(f"Task is currently running in {entry['capability']}.")
    
    if recommendations:
        result["recommendations"] = recommendations
    
    return result


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
