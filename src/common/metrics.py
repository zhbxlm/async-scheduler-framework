"""Prometheus metrics instrumentation for business and system metrics."""
from __future__ import annotations

import time
from typing import Optional, Dict, Any
from contextlib import contextmanager
from functools import wraps

# Try to import prometheus_client, but don't fail if not installed
try:
    from prometheus_client import (
        Counter,
        Gauge,
        Histogram,
        Summary,
        generate_latest,
        CONTENT_TYPE_LATEST,
        REGISTRY,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    # Dummy classes for when prometheus_client is not installed
    class DummyMetric:
        def __init__(self, *args, **kwargs):
            pass
        def inc(self, *args, **kwargs):
            pass
        def dec(self, *args, **kwargs):
            pass
        def set(self, *args, **kwargs):
            pass
        def observe(self, *args, **kwargs):
            pass
        def labels(self, *args, **kwargs):
            return self
    
    Counter = Gauge = Histogram = Summary = DummyMetric
    generate_latest = lambda: b''
    CONTENT_TYPE_LATEST = 'text/plain; version=0.0.4'
    REGISTRY = None


# ---------------------------------------------------------------------------
# Business metrics
# ---------------------------------------------------------------------------

# Queue depth per capability
QUEUE_PENDING_TASKS = Gauge(
    'scheduler_queue_pending_tasks',
    'Number of pending tasks in queue',
    ['capability']
)

QUEUE_RUNNING_TASKS = Gauge(
    'scheduler_queue_running_tasks',
    'Number of running tasks',
    ['capability']
)

# Task execution metrics
TASK_EXECUTION_DURATION = Histogram(
    'scheduler_task_execution_duration_seconds',
    'Duration of task execution in seconds',
    ['capability', 'status'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
)

TASKS_CREATED = Counter(
    'scheduler_tasks_created_total',
    'Total number of tasks created',
    ['capability', 'priority']
)

TASKS_COMPLETED = Counter(
    'scheduler_tasks_completed_total',
    'Total number of tasks completed',
    ['capability', 'status']  # status: success, failure, timeout
)

# System metrics
REDIS_CONNECTIONS = Gauge(
    'scheduler_redis_connections',
    'Redis connection pool size'
)

MYSQL_CONNECTIONS = Gauge(
    'scheduler_mysql_connections',
    'MySQL connection pool size'
)

# DAG metrics
DAG_EXECUTION_DURATION = Histogram(
    'scheduler_dag_execution_duration_seconds',
    'DAG execution duration in seconds',
    ['dag_id'],
    buckets=[1.0, 5.0, 10.0, 30.0, 60.0, 300.0]
)

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def update_queue_metrics(capability: str, pending_count: int, running_count: int) -> None:
    """Update queue depth metrics for a capability."""
    if PROMETHEUS_AVAILABLE:
        QUEUE_PENDING_TASKS.labels(capability=capability).set(pending_count)
        QUEUE_RUNNING_TASKS.labels(capability=capability).set(running_count)


def record_task_creation(capability: str, priority: str) -> None:
    """Record a task creation event."""
    if PROMETHEUS_AVAILABLE:
        TASKS_CREATED.labels(capability=capability, priority=priority).inc()


def record_task_completion(capability: str, status: str, duration: float) -> None:
    """Record a task completion event."""
    if PROMETHEUS_AVAILABLE:
        TASKS_COMPLETED.labels(capability=capability, status=status).inc()
        TASK_EXECUTION_DURATION.labels(capability=capability, status=status).observe(duration)


@contextmanager
def measure_task_execution(capability: str):
    """Context manager to measure task execution duration."""
    start_time = time.time()
    status = "success"
    try:
        yield
    except Exception:
        status = "failure"
        raise
    finally:
        duration = time.time() - start_time
        if PROMETHEUS_AVAILABLE:
            TASK_EXECUTION_DURATION.labels(capability=capability, status=status).observe(duration)


@contextmanager
def measure_dag_execution(dag_id: str):
    """Context manager to measure DAG execution duration."""
    start_time = time.time()
    try:
        yield
    finally:
        duration = time.time() - start_time
        if PROMETHEUS_AVAILABLE:
            DAG_EXECUTION_DURATION.labels(dag_id=dag_id).observe(duration)


def task_execution_timer(func):
    """Decorator to measure task execution duration."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # Try to extract capability from args or kwargs
        capability = kwargs.get('capability', 'unknown')
        if hasattr(args[0], 'capability') if args else False:
            capability = args[0].capability
        
        start_time = time.time()
        status = "success"
        try:
            return await func(*args, **kwargs)
        except Exception:
            status = "failure"
            raise
        finally:
            duration = time.time() - start_time
            if PROMETHEUS_AVAILABLE:
                TASK_EXECUTION_DURATION.labels(capability=capability, status=status).observe(duration)
    
    return wrapper


def get_metrics_response():
    """Return Prometheus metrics in text format."""
    if not PROMETHEUS_AVAILABLE:
        return b'# Prometheus client not installed\n'
    return generate_latest()


# ---------------------------------------------------------------------------
# Integration with existing components
# ---------------------------------------------------------------------------

class MetricsCollector:
    """Collect and update metrics from system components."""
    
    def __init__(self, redis_client=None, queue_manager=None):
        self.redis = redis_client
        self.queue_manager = queue_manager
    
    async def collect_queue_metrics(self) -> None:
        """Collect queue depth metrics for all capabilities."""
        if not PROMETHEUS_AVAILABLE or not self.queue_manager:
            return
        
        try:
            # Get all capabilities
            capabilities = await self.queue_manager._r.smembers("queue:capabilities:registry")
            for capability in capabilities:
                snapshot = await self.queue_manager.get_queue_snapshot(capability)
                pending = snapshot.get('pending', {}).get('size', 0)
                running = snapshot.get('running', {}).get('size', 0)
                update_queue_metrics(capability, pending, running)
        except Exception as e:
            # Don't let metrics collection break the system
            pass
    
    async def collect_redis_metrics(self) -> None:
        """Collect Redis connection pool metrics."""
        if not PROMETHEUS_AVAILABLE or not self.redis:
            return
        
        try:
            # This would need redis-py's connection pool info
            # For now, just set a placeholder
            REDIS_CONNECTIONS.set(1)
        except Exception:
            pass