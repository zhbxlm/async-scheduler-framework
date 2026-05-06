"""Prometheus metrics collection for scheduler.

Exposes metrics for:
- Request counts and latency by endpoint
- Queue depths per capability
- Database connection pool stats
- Task execution success/failure rates
- Redis operations and latency
"""
from __future__ import annotations

import time
from typing import Any, Callable, Optional

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    generate_latest,
    REGISTRY,
    CollectorRegistry,
)


# Global metrics registry
_metrics_registry: Optional[CollectorRegistry] = None


def get_metrics_registry() -> CollectorRegistry:
    """Get or create Prometheus metrics registry."""
    global _metrics_registry
    if _metrics_registry is None:
        _metrics_registry = CollectorRegistry()
    return _metrics_registry


# -------------------------------------------------------------------
# HTTP Request Metrics
# -------------------------------------------------------------------

REQUEST_COUNT = Counter(
    "scheduler_http_requests_total",
    "Total HTTP requests by method and endpoint",
    ["method", "endpoint", "status_code"],
    registry=get_metrics_registry(),
)

REQUEST_LATENCY = Histogram(
    "scheduler_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
    registry=get_metrics_registry(),
)

# -------------------------------------------------------------------
# Queue Metrics
# -------------------------------------------------------------------

QUEUE_DEPTH = Gauge(
    "scheduler_queue_depth",
    "Current queue depth per capability",
    ["capability"],
    registry=get_metrics_registry(),
)

QUEUE_RUNNING = Gauge(
    "scheduler_queue_running",
    "Currently running tasks per capability",
    ["capability"],
    registry=get_metrics_registry(),
)

QUEUE_MAX_CONCURRENT = Gauge(
    "scheduler_queue_max_concurrent",
    "Maximum concurrent tasks per capability",
    ["capability"],
    registry=get_metrics_registry(),
)

# -------------------------------------------------------------------
# Task Execution Metrics
# -------------------------------------------------------------------

TASK_CREATED = Counter(
    "scheduler_tasks_created_total",
    "Total tasks created",
    ["task_type", "tenant_id"],
    registry=get_metrics_registry(),
)

TASK_COMPLETED = Counter(
    "scheduler_tasks_completed_total",
    "Total tasks completed by status",
    ["status", "task_type", "tenant_id"],
    registry=get_metrics_registry(),
)

TASK_EXECUTION_TIME = Histogram(
    "scheduler_task_execution_duration_seconds",
    "Task execution time in seconds",
    ["task_type"],
    buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0],
    registry=get_metrics_registry(),
)

# -------------------------------------------------------------------
# Database Metrics
# -------------------------------------------------------------------

DB_CONNECTIONS = Gauge(
    "scheduler_db_connections",
    "Database connections in pool",
    ["state"],  # idle, active
    registry=get_metrics_registry(),
)

DB_QUERY_DURATION = Histogram(
    "scheduler_db_query_duration_seconds",
    "Database query duration in seconds",
    ["operation"],  # select, insert, update, delete
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
    registry=get_metrics_registry(),
)

# -------------------------------------------------------------------
# Redis Metrics
# -------------------------------------------------------------------

REDIS_OPERATIONS = Counter(
    "scheduler_redis_operations_total",
    "Total Redis operations by type",
    ["operation"],  # get, set, hget, hset, etc.
    registry=get_metrics_registry(),
)

REDIS_LATENCY = Histogram(
    "scheduler_redis_latency_seconds",
    "Redis operation latency in seconds",
    ["operation"],
    buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05],
    registry=get_metrics_registry(),
)

# -------------------------------------------------------------------
# System Metrics
# -------------------------------------------------------------------

ACTIVE_TASKS = Gauge(
    "scheduler_active_tasks",
    "Currently active tasks in the system",
    registry=get_metrics_registry(),
)

RECONCILER_CYCLES = Counter(
    "scheduler_reconciler_cycles_total",
    "Total reconciler cycles executed",
    registry=get_metrics_registry(),
)

RECONCILER_STUCK_TASKS = Gauge(
    "scheduler_reconciler_stuck_tasks",
    "Number of stuck tasks detected in last cycle",
    registry=get_metrics_registry(),
)


# -------------------------------------------------------------------
# Utility Functions
# -------------------------------------------------------------------

class Timer:
    """Context manager for timing operations and recording to Prometheus."""
    
    def __init__(self, histogram: Histogram, labels: dict[str, str] | None = None):
        self.histogram = histogram
        self.labels = labels or {}
        self.start_time: Optional[float] = None
        
    def __enter__(self):
        self.start_time = time.time()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time is not None:
            duration = time.time() - self.start_time
            self.histogram.labels(**self.labels).observe(duration)


def record_redis_operation(operation: str, duration: float) -> None:
    """Record Redis operation metrics."""
    REDIS_OPERATIONS.labels(operation=operation).inc()
    REDIS_LATENCY.labels(operation=operation).observe(duration)


def record_db_operation(operation: str, duration: float) -> None:
    """Record database operation metrics."""
    DB_QUERY_DURATION.labels(operation=operation).observe(duration)


def update_queue_metrics(capability: str, pending: int, running: int, max_concurrent: int) -> None:
    """Update queue metrics for a capability."""
    QUEUE_DEPTH.labels(capability=capability).set(pending)
    QUEUE_RUNNING.labels(capability=capability).set(running)
    QUEUE_MAX_CONCURRENT.labels(capability=capability).set(max_concurrent)


def generate_metrics() -> bytes:
    """Generate Prometheus metrics output."""
    return generate_latest(get_metrics_registry())