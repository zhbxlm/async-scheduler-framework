"""Health check and metrics endpoints."""
from __future__ import annotations
from datetime import datetime
import psutil
import time
from typing import Dict, Any, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from src.common.error_handling import log_errors

router = APIRouter(prefix="/health", tags=["health"])

# Global health state
_start_time = time.time()
_request_count = 0


@router.get("/")
async def health_overview() -> Dict[str, Any]:
    """Basic health check."""
    global _request_count
    _request_count += 1
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "uptime_seconds": int(time.time() - _start_time),
        "request_count": _request_count,
        "service": "async-scheduler-framework",
        "version": "1.0.0",
    }


@router.get("/detailed")
@log_errors(log_level="WARNING", raise_exception=False)
async def health_detailed() -> Dict[str, Any]:
    """Detailed health check with system metrics."""
    try:
        # System metrics
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # Process metrics
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
                "connections": len(process.connections()),
            },
            "uptime_seconds": int(time.time() - _start_time),
            "request_count": _request_count,
        }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Health check failed: {str(e)}"
        )


@router.get("/ready")
async def readiness_probe() -> Dict[str, Any]:
    """Readiness probe for Kubernetes/load balancers."""
    # Add service-specific readiness checks here
    # e.g., check Redis, MySQL connections
    
    return {
        "status": "ready",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "checks": {
            "api": "ok",
            # "redis": "ok" if redis_ok else "degraded",
            # "mysql": "ok" if mysql_ok else "degraded",
        }
    }


@router.get("/metrics")
async def metrics_prometheus() -> str:
    """Prometheus metrics endpoint (text format)."""
    # Simple metrics for now, can be expanded with prometheus_client
    process = psutil.Process()
    
    metrics = []
    metrics.append(f"# HELP scheduler_uptime_seconds Uptime of scheduler service")
    metrics.append(f"# TYPE scheduler_uptime_seconds gauge")
    metrics.append(f"scheduler_uptime_seconds {time.time() - _start_time}")
    
    metrics.append(f"# HELP scheduler_request_count Total HTTP requests")
    metrics.append(f"# TYPE scheduler_request_count counter")
    metrics.append(f"scheduler_request_count {_request_count}")
    
    metrics.append(f"# HELP scheduler_memory_rss_bytes Resident memory usage")
    metrics.append(f"# TYPE scheduler_memory_rss_bytes gauge")
    metrics.append(f"scheduler_memory_rss_bytes {process.memory_info().rss}")
    
    metrics.append(f"# HELP scheduler_cpu_seconds_total CPU time used")
    metrics.append(f"# TYPE scheduler_cpu_seconds_total counter")
    cpu_times = process.cpu_times()
    metrics.append(f"scheduler_cpu_seconds_total {cpu_times.user + cpu_times.system}")
    
    return "\n".join(metrics)


# Health check endpoints for external services
@router.get("/redis")
@log_errors(log_level="ERROR", raise_exception=False)
async def health_redis() -> Dict[str, Any]:
    """Redis health check."""
    # This would check Redis connection
    # For now, return placeholder
    return {
        "status": "healthy",
        "service": "redis",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "note": "Redis check not implemented - requires app context"
    }


@router.get("/mysql")
@log_errors(log_level="ERROR", raise_exception=False)
async def health_mysql() -> Dict[str, Any]:
    """MySQL health check."""
    # This would check MySQL connection
    return {
        "status": "healthy",
        "service": "mysql",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "note": "MySQL check not implemented - requires app context"
    }