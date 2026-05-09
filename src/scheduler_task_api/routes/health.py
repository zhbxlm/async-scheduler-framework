"""Health check and metrics endpoints for task-api package."""
from __future__ import annotations

import asyncio
import time
from datetime import datetime

import psutil
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from sqlalchemy import text

from scheduler_task_api.error_handling import log_errors
from src.monitoring.metrics import generate_metrics

router = APIRouter(prefix="/health", tags=["health"])

_start_time = time.time()
_request_count = 0


@router.get("/")
async def health_overview() -> dict[str, object]:
    global _request_count
    _request_count += 1
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "uptime_seconds": int(time.time() - _start_time),
        "request_count": _request_count,
        "service": "async-scheduler-task-api",
        "version": "1.0.0",
    }


@router.get("/detailed")
@log_errors(log_level="WARNING", raise_exception=False)
async def health_detailed() -> dict[str, object]:
    try:
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage("/")
        process = psutil.Process()
        process_memory = process.memory_info()
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "system": {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "memory_available_gb": round(memory.available / (1024**3), 2),
                "disk_percent": disk.percent,
                "disk_free_gb": round(disk.free / (1024**3), 2),
            },
            "process": {
                "pid": process.pid,
                "memory_rss_mb": round(process_memory.rss / (1024**2), 2),
                "memory_vms_mb": round(process_memory.vms / (1024**2), 2),
                "cpu_percent": process.cpu_percent(),
                "threads": process.num_threads(),
                "connections": len(process.net_connections()),
            },
            "uptime_seconds": int(time.time() - _start_time),
            "request_count": _request_count,
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")


@router.get("/ready")
async def readiness_probe(request: Request) -> dict[str, object]:
    checks: dict[str, str] = {"api": "ok"}
    overall = "ready"

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is not None:
        try:
            await asyncio.wait_for(redis_client.ping(), timeout=2.0)
            checks["redis"] = "ok"
        except Exception:
            checks["redis"] = "unreachable"
            overall = "not_ready"
    else:
        checks["redis"] = "not_configured"

    engine = getattr(request.app.state, "async_engine", None)
    if engine is not None:
        try:
            async with engine.connect() as conn:
                await asyncio.wait_for(conn.execute(text("SELECT 1")), timeout=2.0)
            checks["mysql"] = "ok"
        except Exception:
            checks["mysql"] = "unreachable"
            overall = "not_ready"

    status_code = 200 if overall == "ready" else 503
    result = {"status": overall, "timestamp": datetime.utcnow().isoformat() + "Z", "checks": checks}
    if status_code != 200:
        raise HTTPException(status_code=status_code, detail=result)
    return result


@router.get("/metrics", response_class=Response)
async def metrics_prometheus() -> Response:
    process = psutil.Process()
    simple_metrics = []
    simple_metrics.append("# HELP scheduler_uptime_seconds Uptime of scheduler service")
    simple_metrics.append("# TYPE scheduler_uptime_seconds gauge")
    simple_metrics.append(f"scheduler_uptime_seconds {time.time() - _start_time}")
    simple_metrics.append("# HELP scheduler_request_count Total HTTP requests")
    simple_metrics.append("# TYPE scheduler_request_count counter")
    simple_metrics.append(f"scheduler_request_count {_request_count}")
    simple_metrics.append("# HELP scheduler_memory_rss_bytes Resident memory usage")
    simple_metrics.append("# TYPE scheduler_memory_rss_bytes gauge")
    simple_metrics.append(f"scheduler_memory_rss_bytes {process.memory_info().rss}")
    simple_metrics.append("# HELP scheduler_cpu_seconds_total CPU time used")
    simple_metrics.append("# TYPE scheduler_cpu_seconds_total counter")
    cpu_times = process.cpu_times()
    simple_metrics.append(f"scheduler_cpu_seconds_total {cpu_times.user + cpu_times.system}")
    prometheus_data = generate_metrics()
    content = "\n".join(simple_metrics) + "\n"
    if prometheus_data:
        content += prometheus_data.decode("utf-8")
    return Response(content=content, media_type="text/plain; version=0.0.4")


@router.get("/redis")
async def health_redis(request: Request) -> dict[str, object]:
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return {"status": "unknown", "service": "redis", "timestamp": datetime.utcnow().isoformat() + "Z", "note": "Redis not configured"}
    try:
        await asyncio.wait_for(redis_client.ping(), timeout=2.0)
        return {"status": "healthy", "service": "redis", "timestamp": datetime.utcnow().isoformat() + "Z"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unreachable: {e}")


@router.get("/mysql")
async def health_mysql(request: Request) -> dict[str, object]:
    engine = getattr(request.app.state, "async_engine", None)
    if engine is None:
        return {"status": "disabled", "service": "mysql", "timestamp": datetime.utcnow().isoformat() + "Z", "note": "MySQL not configured"}
    try:
        async with engine.connect() as conn:
            await asyncio.wait_for(conn.execute(text("SELECT 1")), timeout=2.0)
        return {"status": "healthy", "service": "mysql", "timestamp": datetime.utcnow().isoformat() + "Z"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"MySQL unreachable: {e}")


@router.get("/platform")
async def health_platform(request: Request) -> dict[str, object]:
    return {
        "status": "partial",
        "note": "platform-level operator observability remains framework-owned during transition",
    }
