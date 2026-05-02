"""FastAPI task service for managing tasks, schedules, and DAGs."""

import asyncio
import json as _json
import logging
import os
import time
from asyncio import Queue as _Queue
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel, Field

from async_scheduler.core.models import (
    DAG,
    DAGCreate,
    DAGNode,
    DAGNodeExecution,
    ExecutionAttempt,
    ExecutionAttemptStatus,
    Schedule,
    ScheduleCreate,
    ScheduleStatus,
    Task,
    TaskCreate,
    TaskStatus,
)
from async_scheduler.core.models import Tenant, TenantCreate, TenantUpdate
from async_scheduler.persistence import (
    DAGRepository,
    ExecutionAttemptRepository,
    ScheduleRepository,
    TaskRepository,
    TenantRepository,
    get_session,
    get_session_no_context,
)
from async_scheduler.platform import QuotaExceededError
from async_scheduler.platform import ServiceContainer, build_service_container

logger = logging.getLogger(__name__)

_APP_START_TIME: float = time.time()  # O6: uptime tracking
_APP_VERSION = "1.0.0"

# Global service container
services: ServiceContainer | None = None


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _get_services() -> ServiceContainer:
    """FastAPI dependency: resolve global service container or raise 503."""
    if services is None:
        raise HTTPException(
            status_code=503,
            detail={"error_code": "SERVICE_UNAVAILABLE", "message": "Service container not yet initialized"},
        )
    return services


async def _collect_lease_snapshot(
    session,
    svc: ServiceContainer,
    limit: int = 200,
    offset: int = 0,
    worker_id: str | None = None,
) -> list[dict]:
    """Shared lease-scan helper used by debug/summary, anomalies, and anomaly-summary.

    Uses a single JOIN query to fetch attempts + task status, avoiding N+1 DB roundtrips.
    Returns a list of dicts with keys:
        task_id, task_status, worker_id, attempt_status, locked, lease,
        attempt (full attempt detail dict)
    """
    rows = await ExecutionAttemptRepository.list_latest_attempts_with_task(
        session, limit=limit, offset=offset, worker_id=worker_id
    )
    items: list[dict] = []
    for attempt, task in rows:
        lease = None
        if svc.lock_backend is not None and hasattr(svc.lock_backend, "describe_lock"):
            lease = await svc.lock_backend.describe_lock(f"task:{attempt.task_id}")
        lease_locked = bool(lease and lease.get("locked"))
        task_status = None if task is None else task.status
        ttl_ms = None if lease is None else lease.get("ttl_ms")

        anomaly_types: list[str] = []
        if task_status == TaskStatus.RUNNING and not lease_locked:
            anomaly_types.append("running_without_lock")
        if task_status in {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT} and lease_locked:
            anomaly_types.append("locked_but_terminal")
        if attempt.status == ExecutionAttemptStatus.ABANDONED and task_status == TaskStatus.RUNNING:
            anomaly_types.append("abandoned_but_running")
        if lease_locked and ttl_ms is not None and 0 < ttl_ms < 5000:
            anomaly_types.append("stale_lease")

        items.append({
            "task_id": attempt.task_id,
            "task_status": task_status,
            "worker_id": attempt.worker_id,
            "attempt_status": attempt.status,
            "locked": lease_locked,
            "lease": lease,
            "anomaly_types": anomaly_types,
            "attempt": {
                "id": attempt.id,
                "worker_id": attempt.worker_id,
                "retry_index": attempt.retry_index,
                "status": attempt.status,
                "lease_token": attempt.lease_token,
                "started_at": attempt.started_at,
                "last_heartbeat_at": attempt.last_heartbeat_at,
                "completed_at": attempt.completed_at,
            },
        })
    return items


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown."""
    global services

    # Ensure structured logging is configured even when started via uvicorn directly
    from async_scheduler.observability import configure_logging as _configure_logging
    _configure_logging(
        service=os.environ.get("SERVICE_NAME", "scheduler-api"),
        node_id=os.environ.get("NODE_ID"),
        version=os.environ.get("SERVICE_VERSION"),
    )

    logger.info("Starting Async Scheduler API...")

    services = await build_service_container()

    # Start background processes
    await services.task_consumer.start()
    await services.cron_scheduler.start()
    await services.reconciler.start()

    logger.info("Async Scheduler API started")

    yield

    # Shutdown
    logger.info("Shutting down Async Scheduler API...")
    if services is not None:
        await services.task_consumer.stop()
        await services.cron_scheduler.stop()
        await services.reconciler.stop()
    logger.info("Async Scheduler API shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Async Scheduler API",
    description="Async scheduling framework with DAG execution, task queue, and cron scheduling",
    version="0.1.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request-ID middleware — injects X-Request-ID into every log record
# ---------------------------------------------------------------------------

import uuid as _uuid
from async_scheduler.observability import _ctx_request_id as _log_request_id


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Attach a unique request ID to every request and propagate it via contextvars.

    The request ID is taken from the incoming X-Request-ID header (for distributed
    tracing continuity) or generated fresh. It is returned in the response header
    and automatically injected into all log records for the duration of the request.
    """

    async def dispatch(self, request: Request, call_next):
        req_id = request.headers.get("X-Request-ID") or str(_uuid.uuid4())
        token = _log_request_id.set(req_id)
        try:
            response = await call_next(request)
            response.headers["X-Request-ID"] = req_id
            return response
        finally:
            _log_request_id.reset(token)


app.add_middleware(RequestContextMiddleware)


# Pydantic request/response models - simple aliases; extend when response shapes diverge
TaskResponse = Task
ScheduleResponse = Schedule
DAGResponse = DAG


class DAGCreateRequest(BaseModel):
    """DAG creation request."""
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    nodes: list[DAGNode] = Field(default_factory=list)
    tenant_id: str | None = None
    max_parallelism: int = Field(default=4, ge=1)


class DAGExecuteRequest(BaseModel):
    """DAG execution request."""
    dag_id: str


class DAGExecuteResponse(BaseModel):
    """DAG execution response."""
    dag: DAGResponse
    status: str


ExecutionAttemptResponse = ExecutionAttempt


# API Routes


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "Async Scheduler API",
        "version": "0.1.0",
        "status": "running",
    }


@app.get("/health")
async def health():
    """Health check: returns a simple status signal.

    - ``status``: ``"healthy"`` | ``"degraded"`` | ``"starting"``
    - ``issues``: list of degraded sub-systems (empty when healthy)

    For full internal metrics, use ``/health/detail``.
    """
    ready = services is not None
    if not ready:
        return {"status": "starting", "issues": ["service_container_not_initialized"]}

    issues: list[str] = []
    if not services.task_consumer.is_running():
        issues.append("consumer_not_running")
    if not services.cron_scheduler.is_running():
        issues.append("scheduler_not_running")
    if not services.reconciler.is_running():
        issues.append("reconciler_not_running")

    return {
        "status": "degraded" if issues else "healthy",
        "issues": issues,
        "version": _APP_VERSION,
        "uptime_seconds": round(time.time() - _APP_START_TIME, 1),
    }


@app.get("/health/detail")
async def health_detail():
    """Detailed health: full internal metrics for operators and dashboards."""
    worker_count = 0
    if services and services.worker_registry is not None:
        worker_count = len(await services.worker_registry.list_workers(include_stale=False))
    repair_history_count = 0
    if services:
        repair_history_count = len(services.reconciler.list_repair_history(limit=1000, offset=0))

    ready = services is not None
    uptime_seconds = round(time.time() - _APP_START_TIME, 1)

    return {
        "status": "healthy" if ready else "starting",
        "ready": ready,
        "version": _APP_VERSION,
        "uptime_seconds": uptime_seconds,
        "queue_size": await services.queue_manager.get_queue_count() if services else 0,
        "scheduled_count": await services.queue_manager.get_scheduled_count() if services else 0,
        "consumer_running": services.task_consumer.is_running() if services else False,
        "scheduler_running": services.cron_scheduler.is_running() if services else False,
        "reconciler_running": services.reconciler.is_running() if services else False,
        "worker_count": worker_count,
        "repair_history_count": repair_history_count,
    }


@app.get("/readiness")
async def readiness():
    """Kubernetes readiness probe: 200 when services are initialized."""
    if services is None:
        raise HTTPException(status_code=503, detail="Services not yet initialized")
    return {"ready": True}


@app.get("/liveness")
async def liveness():
    """Kubernetes liveness probe: always 200 while process is running."""
    return {"alive": True, "uptime_seconds": round(time.time() - _APP_START_TIME, 1)}


# Task endpoints


@app.post("/tasks", response_model=TaskResponse, status_code=201)
async def create_task(task: TaskCreate, svc: ServiceContainer = Depends(_get_services)):
    """Create a new task."""
    async with get_session() as session:
        try:
            db_task = await svc.task_router.create_task(task)
        except QuotaExceededError as e:
            raise HTTPException(status_code=429, detail={"error_code": "QUOTA_EXCEEDED", "message": str(e)}) from e
        return TaskResponse.model_validate(db_task)


class BatchTaskCreate(BaseModel):
    """Request body for batch task creation."""
    tasks: list[TaskCreate] = Field(..., min_length=1, max_length=100)


class BatchTaskResponse(BaseModel):
    """Response for batch task creation."""
    created: list[TaskResponse]
    failed: list[dict]  # {index, error}
    total: int
    succeeded: int


@app.post("/tasks/batch", response_model=BatchTaskResponse, status_code=201)
async def create_tasks_batch(body: BatchTaskCreate, svc: ServiceContainer = Depends(_get_services)):
    """U7: Batch create up to 100 tasks atomically (best-effort; partial failures reported)."""
    created: list[TaskResponse] = []
    failed: list[dict] = []
    for idx, task_create in enumerate(body.tasks):
        try:
            db_task = await svc.task_router.create_task(task_create)
            created.append(TaskResponse.model_validate(db_task))
        except QuotaExceededError as e:
            failed.append({"index": idx, "error": f"quota_exceeded: {e}", "error_code": "QUOTA_EXCEEDED"})
        except Exception as e:
            failed.append({"index": idx, "error": str(e), "error_code": "CREATE_FAILED"})
    return BatchTaskResponse(
        created=created,
        failed=failed,
        total=len(body.tasks),
        succeeded=len(created),
    )


@app.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(task_id: str):
    """Get a task by ID."""
    async with get_session() as session:
        task = await TaskRepository.get(session, task_id)
        if not task:
            raise HTTPException(status_code=404, detail={"error_code": "TASK_NOT_FOUND", "message": "Task not found"})
        return TaskResponse.model_validate(task)


@app.get("/tasks", response_model=list[TaskResponse])
async def list_tasks(
    status: TaskStatus | None = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """List all tasks, optionally filtered by status."""
    async with get_session() as session:
        tasks = await TaskRepository.list_all(session, status=status, limit=limit, offset=offset)
        return [TaskResponse.model_validate(t) for t in tasks]


@app.get("/tasks/{task_id}/attempts", response_model=list[ExecutionAttemptResponse])
async def list_attempts_for_task(
    task_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """List execution attempts for a task."""
    async with get_session() as session:
        task = await TaskRepository.get(session, task_id)
        if not task:
            raise HTTPException(status_code=404, detail={"error_code": "TASK_NOT_FOUND", "message": "Task not found"})
        attempts = await ExecutionAttemptRepository.list_for_task(session, task_id, limit=limit, offset=offset)
        return [ExecutionAttemptResponse.model_validate(attempt) for attempt in attempts]


@app.get("/tasks/{task_id}/attempts/latest", response_model=ExecutionAttemptResponse)
async def get_latest_attempt(task_id: str):
    """Get the latest execution attempt for a task."""
    async with get_session() as session:
        task = await TaskRepository.get(session, task_id)
        if not task:
            raise HTTPException(status_code=404, detail={"error_code": "TASK_NOT_FOUND", "message": "Task not found"})
        attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task_id)
        if not attempt:
            raise HTTPException(status_code=404, detail={"error_code": "ATTEMPT_NOT_FOUND", "message": "No execution attempt found for this task"})
        return ExecutionAttemptResponse.model_validate(attempt)


@app.get("/tasks/{task_id}/history")
async def get_task_history(
    task_id: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """PM10: Full execution history for a task: task metadata + all attempts."""
    async with get_session() as session:
        task = await TaskRepository.get(session, task_id)
        if not task:
            raise HTTPException(
                status_code=404,
                detail={"message": "Task not found", "error_code": "TASK_NOT_FOUND", "task_id": task_id},
            )
        attempts = await ExecutionAttemptRepository.list_for_task(session, task_id, limit=limit, offset=offset)
        return {
            "task": TaskResponse.model_validate(task),
            "attempts": [ExecutionAttemptResponse.model_validate(a) for a in attempts],
            "attempt_count": len(attempts),
            "summary": {
                "total_attempts": len(attempts),
                "succeeded": sum(1 for a in attempts if a.status and "succeed" in str(a.status).lower()),
                "failed": sum(1 for a in attempts if a.status and "fail" in str(a.status).lower()),
                "final_status": task.status,
            },
        }


@app.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str, svc: ServiceContainer = Depends(_get_services)):
    """Cancel a task."""
    await svc.queue_manager.cancel(task_id)
    await svc.task_executor.cancel(task_id)

    async with get_session() as session:
        task = await TaskRepository.update(session, task_id, status=TaskStatus.CANCELLED)
        if not task:
            raise HTTPException(status_code=404, detail={"error_code": "TASK_NOT_FOUND", "message": "Task not found"})

    return {"message": "Task cancelled", "task_id": task_id}


@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    """Delete a task."""
    async with get_session() as session:
        deleted = await TaskRepository.delete(session, task_id)
        if not deleted:
            raise HTTPException(status_code=404, detail={"error_code": "TASK_NOT_FOUND", "message": "Task not found"})
    return {"message": "Task deleted", "task_id": task_id}


# Schedule endpoints


@app.post("/schedules", response_model=ScheduleResponse, status_code=201)
async def create_schedule(schedule: ScheduleCreate):
    """Create a new schedule."""
    async with get_session() as session:
        db_schedule = await ScheduleRepository.create(session, schedule)
        return ScheduleResponse.model_validate(db_schedule)


@app.get("/schedules/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule(schedule_id: str):
    """Get a schedule by ID."""
    async with get_session() as session:
        schedule = await ScheduleRepository.get(session, schedule_id)
        if not schedule:
            raise HTTPException(status_code=404, detail={"error_code": "SCHEDULE_NOT_FOUND", "message": "Schedule not found"})
        return ScheduleResponse.model_validate(schedule)


@app.get("/schedules", response_model=list[ScheduleResponse])
async def list_schedules(limit: int = Query(100, ge=1, le=1000)):
    """List all schedules."""
    async with get_session() as session:
        schedules = await ScheduleRepository.list_active(session, limit=limit)
        return [ScheduleResponse.model_validate(s) for s in schedules]


@app.post("/schedules/{schedule_id}/trigger")
async def trigger_schedule(schedule_id: str, svc: ServiceContainer = Depends(_get_services)):
    """Trigger a schedule execution immediately."""
    triggered = await svc.cron_scheduler.trigger_schedule(schedule_id)
    if not triggered:
        raise HTTPException(status_code=404, detail={"error_code": "SCHEDULE_NOT_FOUND", "message": "Schedule not found or not active"})
    return {"message": "Schedule triggered", "schedule_id": schedule_id}


@app.post("/schedules/{schedule_id}/pause", response_model=ScheduleResponse)
async def pause_schedule(schedule_id: str):
    from async_scheduler.scheduler import ScheduleRegistry

    registry = ScheduleRegistry()
    schedule = await registry.pause(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail={"error_code": "SCHEDULE_NOT_FOUND", "message": "Schedule not found"})
    return ScheduleResponse.model_validate(schedule)


@app.post("/schedules/{schedule_id}/resume", response_model=ScheduleResponse)
async def resume_schedule(schedule_id: str):
    from async_scheduler.scheduler import ScheduleRegistry

    registry = ScheduleRegistry()
    schedule = await registry.resume(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail={"error_code": "SCHEDULE_NOT_FOUND", "message": "Schedule not found"})
    return ScheduleResponse.model_validate(schedule)


# DAG endpoints


@app.post("/dags", response_model=DAGResponse, status_code=201)
async def create_dag(request: DAGCreateRequest):
    """Create a new DAG."""
    from async_scheduler.persistence import DAGRepository

    dag_create = DAGCreate(
        name=request.name,
        description=request.description,
        nodes=request.nodes,
        tenant_id=request.tenant_id,
        max_parallelism=request.max_parallelism,
    )

    async with get_session() as session:
        db_dag = await DAGRepository.create(session, dag_create)
        return DAGResponse.model_validate(db_dag)


@app.get("/dags/{dag_id}", response_model=DAGResponse)
async def get_dag(dag_id: str):
    """Get a DAG by ID."""
    from async_scheduler.persistence import DAGRepository

    async with get_session() as session:
        dag = await DAGRepository.get(session, dag_id)
        if not dag:
            raise HTTPException(status_code=404, detail={"error_code": "DAG_NOT_FOUND", "message": "DAG not found"})
        return DAGResponse.model_validate(dag)


@app.get("/dags", response_model=list[DAGResponse])
async def list_dags(limit: int = Query(100, ge=1, le=1000)):
    """List all DAGs."""
    from async_scheduler.persistence import DAGRepository

    async with get_session() as session:
        dags = await DAGRepository.list_all(session, limit=limit)
        return [DAGResponse.model_validate(d) for d in dags]


@app.post("/dags/execute", response_model=DAGExecuteResponse)
async def execute_dag(request: DAGExecuteRequest, svc: ServiceContainer = Depends(_get_services)):
    """Execute a DAG."""
    async with get_session() as session:
        dag = await DAGRepository.get(session, request.dag_id)
        if not dag:
            raise HTTPException(status_code=404, detail={"error_code": "DAG_NOT_FOUND", "message": "DAG not found"})

    async with get_session() as session:
        await DAGRepository.update(
            session,
            dag.id,
            status="running",
            started_at=datetime.utcnow(),
        )

    async def _execute():
        result_dag = await svc.dag_engine.execute(dag, svc.dag_handler)
        async with get_session() as session:
            await DAGRepository.update(
                session,
                dag.id,
                status=result_dag.status,
                node_executions=result_dag.node_executions,
                context=result_dag.context,
                started_at=result_dag.started_at,
                completed_at=result_dag.completed_at,
            )

    asyncio.create_task(_execute())

    return DAGExecuteResponse(
        dag=DAGResponse.model_validate(dag),
        status="executing",
    )


@app.post("/dags/{dag_id}/cancel")
async def cancel_dag(dag_id: str, svc: ServiceContainer = Depends(_get_services)):
    """Cancel a running DAG execution."""
    cancelled = await svc.dag_engine.cancel(dag_id)
    if not cancelled:
        raise HTTPException(status_code=404, detail={"error_code": "DAG_NOT_FOUND", "message": "DAG not found or not running"})
    return {"message": "DAG cancelled", "dag_id": dag_id}


# Queue endpoints


@app.get("/queue/stats")
async def queue_stats(svc: ServiceContainer = Depends(_get_services)):
    """Get queue statistics."""
    sizes = await svc.queue_manager.size()
    worker_count = 0
    if svc.worker_registry is not None:
        worker_count = len(await svc.worker_registry.list_workers(include_stale=False))

    return {
        "queue_sizes": sizes,
        "total_queued": await svc.queue_manager.get_queue_count(),
        "scheduled_count": await svc.queue_manager.get_scheduled_count(),
        "running_tasks": svc.task_executor.get_running_count(),
        "worker_count": worker_count,
        "reconciler_running": svc.reconciler.is_running(),
    }


@app.get("/debug/summary")
async def debug_summary(svc: ServiceContainer = Depends(_get_services)):
    queue_sizes = await svc.queue_manager.size()
    workers = []
    if svc.worker_registry is not None:
        workers = await svc.worker_registry.list_workers(include_stale=True)
    repair_metrics = svc.reconciler.get_metrics()
    repair_history = svc.reconciler.list_repair_history(limit=10, offset=0)

    async with await get_session_no_context() as session:
        lease_items = await _collect_lease_snapshot(session, svc, limit=200)

    locked_count = sum(1 for i in lease_items if i["locked"])
    running_with_lock = sum(1 for i in lease_items if i["task_status"] == TaskStatus.RUNNING and i["locked"])
    anomaly_counts = {k: 0 for k in ("running_without_lock", "locked_but_terminal", "abandoned_but_running", "stale_lease")}
    for item in lease_items:
        for a in item["anomaly_types"]:
            if a in anomaly_counts:
                anomaly_counts[a] += 1

    return {
        "health": {
            "consumer_running": svc.task_consumer.is_running(),
            "scheduler_running": svc.cron_scheduler.is_running(),
            "reconciler_running": svc.reconciler.is_running(),
        },
        "queue": {
            "sizes": queue_sizes,
            "total_queued": await svc.queue_manager.get_queue_count(),
            "scheduled_count": await svc.queue_manager.get_scheduled_count(),
            "running_tasks": svc.task_executor.get_running_count(),
        },
        "workers": {
            "count": len(workers),
            "items": [worker.__dict__ for worker in workers],
        },
        "leases": {
            "count": len(lease_items),
            "locked_count": locked_count,
            "running_with_lock_count": running_with_lock,
            "running_without_lock_count": anomaly_counts["running_without_lock"],
            "locked_but_terminal_count": anomaly_counts["locked_but_terminal"],
            "abandoned_but_running_count": anomaly_counts["abandoned_but_running"],
            "stale_lease_count": anomaly_counts["stale_lease"],
            "anomaly_summary": {
                **anomaly_counts,
                "total": sum(anomaly_counts.values()),
                "endpoint": "/debug/leases/anomalies",
                "summaryEndpoint": "/debug/leases/anomalies/summary",
            },
            "items": [{"task_id": i["task_id"], "task_status": i["task_status"],
                       "worker_id": i["worker_id"], "attempt_status": i["attempt_status"],
                       "locked": i["locked"], "lease": i["lease"]} for i in lease_items[:20]],
        },
        "reconciler": {
            "metrics": {
                "total_runs": repair_metrics.total_runs,
                "stuck_tasks_found": repair_metrics.stuck_tasks_found,
                "tasks_repaired": repair_metrics.tasks_repaired,
                "tasks_ignored": repair_metrics.tasks_ignored,
                "tasks_requeued": repair_metrics.tasks_requeued,
                "last_run_at": repair_metrics.last_run_at,
                "last_repaired_count": repair_metrics.last_repaired_count,
                "repair_rate": repair_metrics.get_repair_rate(),
            },
            "recent_history": repair_history,
        },
        "capability_queues": {
            "capabilities": await svc.queue_manager.discover_capabilities(),
        },
    }


@app.get("/quota/stats")
async def quota_stats(svc: ServiceContainer = Depends(_get_services)):
    return svc.quota_manager.stats()


@app.get("/queues/capabilities")
async def list_queue_capabilities(svc: ServiceContainer = Depends(_get_services)):
    """List all known capability queue names."""
    caps = await svc.queue_manager.discover_capabilities()
    return {"capabilities": caps}


@app.get("/queues/{capability}/stats")
async def get_capability_queue_stats(capability: str, svc: ServiceContainer = Depends(_get_services)):
    """Get runtime stats for a specific capability queue."""
    stats = await svc.queue_manager.get_capability_stats(capability)
    return stats.to_dict()


@app.get("/reconciler/stats")
async def reconciler_stats(svc: ServiceContainer = Depends(_get_services)):
    metrics = svc.reconciler.get_metrics()
    return {
        "total_runs": metrics.total_runs,
        "stuck_tasks_found": metrics.stuck_tasks_found,
        "tasks_repaired": metrics.tasks_repaired,
        "tasks_ignored": metrics.tasks_ignored,
        "tasks_requeued": metrics.tasks_requeued,
        "last_run_at": metrics.last_run_at,
        "last_repaired_count": metrics.last_repaired_count,
        "repair_rate": metrics.get_repair_rate(),
        "running": svc.reconciler.is_running(),
    }


@app.get("/workers")
async def list_workers(include_stale: bool = Query(False), svc: ServiceContainer = Depends(_get_services)):
    if svc.worker_registry is None:
        raise HTTPException(status_code=503, detail={"error_code": "WORKER_REGISTRY_UNAVAILABLE", "message": "Worker registry not configured in this deployment"})
    workers = await svc.worker_registry.list_workers(include_stale=include_stale)
    return {
        "items": [worker.__dict__ for worker in workers],
        "count": len(workers),
        "include_stale": include_stale,
    }


@app.get("/workers/{worker_id}")
async def get_worker(worker_id: str, svc: ServiceContainer = Depends(_get_services)):
    if svc.worker_registry is None:
        raise HTTPException(status_code=404, detail="Worker not found")
    workers = await svc.worker_registry.list_workers(include_stale=True)
    for worker in workers:
        if worker.worker_id == worker_id:
            return worker.__dict__
    raise HTTPException(status_code=404, detail="Worker not found")


@app.get("/debug/leases")
async def list_lease_debug(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    worker_id: str | None = Query(None),
    task_status: str | None = Query(None),
    attempt_status: str | None = Query(None),
    locked_only: bool = Query(False),
    svc: ServiceContainer = Depends(_get_services),
):
    async with await get_session_no_context() as session:
        all_items = await _collect_lease_snapshot(session, svc, limit=limit, offset=offset, worker_id=worker_id)

    items = []
    for item in all_items:
        if task_status is not None and (item["task_status"] is None or item["task_status"].value != task_status):
            continue
        if attempt_status is not None and item["attempt_status"].value != attempt_status:
            continue
        if locked_only and not item["locked"]:
            continue
        items.append({
            "task_id": item["task_id"],
            "task_status": item["task_status"],
            "lease": item["lease"],
            "latest_attempt": item["attempt"],
        })

    return {
        "items": items,
        "count": len(items),
        "limit": limit,
        "offset": offset,
        "filters": {
            "worker_id": worker_id,
            "task_status": task_status,
            "attempt_status": attempt_status,
            "locked_only": locked_only,
        },
    }


@app.get("/debug/leases/anomalies")
async def list_lease_anomalies(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    svc: ServiceContainer = Depends(_get_services),
):
    async with await get_session_no_context() as session:
        all_items = await _collect_lease_snapshot(session, svc, limit=limit, offset=offset)

    return {
        "items": [
            {
                "task_id": i["task_id"],
                "task_status": i["task_status"],
                "lease": i["lease"],
                "latest_attempt": i["attempt"],
                "anomaly_types": i["anomaly_types"],
            }
            for i in all_items if i["anomaly_types"]
        ],
        "count": sum(1 for i in all_items if i["anomaly_types"]),
        "limit": limit,
        "offset": offset,
    }


@app.get("/debug/leases/anomalies/summary")
async def get_lease_anomaly_summary(
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    svc: ServiceContainer = Depends(_get_services),
):
    async with await get_session_no_context() as session:
        all_items = await _collect_lease_snapshot(session, svc, limit=limit, offset=offset)

    by_type = {"running_without_lock": 0, "locked_but_terminal": 0, "abandoned_but_running": 0, "stale_lease": 0}
    sample_items: dict[str, list[dict]] = {key: [] for key in by_type}
    for item in all_items:
        for anomaly in item["anomaly_types"]:
            if anomaly in by_type:
                by_type[anomaly] += 1
                if len(sample_items[anomaly]) < 5:
                    sample_items[anomaly].append({
                        "task_id": item["task_id"],
                        "task_status": item["task_status"],
                        "worker_id": item["worker_id"],
                        "attempt_status": item["attempt_status"],
                    })

    return {
        "counts": by_type,
        "samples": sample_items,
        "total": sum(by_type.values()),
        "sourceEndpoint": "/debug/leases/anomalies",
        "limit": limit,
        "offset": offset,
    }


@app.get("/debug/leases/{task_id}")
async def get_task_lease_debug(task_id: str, svc: ServiceContainer = Depends(_get_services)):
    lease_info = None
    if svc.lock_backend is not None and hasattr(svc.lock_backend, "describe_lock"):
        lease_info = await svc.lock_backend.describe_lock(f"task:{task_id}")

    latest_attempt = None
    async with await get_session_no_context() as session:
        task = await TaskRepository.get(session, task_id)
        if task is not None:
            latest_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task_id)

    if latest_attempt is None and task is None:
        raise HTTPException(status_code=404, detail={"error_code": "TASK_NOT_FOUND", "message": "Task not found"})

    return {
        "task_id": task_id,
        "task_status": None if task is None else task.status,
        "lease": lease_info,
        "latest_attempt": None if latest_attempt is None else {
            "id": latest_attempt.id,
            "worker_id": latest_attempt.worker_id,
            "retry_index": latest_attempt.retry_index,
            "status": latest_attempt.status,
            "lease_token": latest_attempt.lease_token,
            "started_at": latest_attempt.started_at,
            "last_heartbeat_at": latest_attempt.last_heartbeat_at,
            "completed_at": latest_attempt.completed_at,
        },
    }


@app.get("/workers/{worker_id}/leases")
async def get_worker_leases(
    worker_id: str,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    svc: ServiceContainer = Depends(_get_services),
):
    async with await get_session_no_context() as session:
        all_items = await _collect_lease_snapshot(session, svc, limit=limit, offset=offset, worker_id=worker_id)

    return {
        "worker_id": worker_id,
        "items": [{"task_id": i["task_id"], "task_status": i["task_status"], "lease": i["lease"], "latest_attempt": i["attempt"]} for i in all_items],
        "count": len(all_items),
        "limit": limit,
        "offset": offset,
    }


@app.post("/reconciler/run")
async def run_reconciler_once(svc: ServiceContainer = Depends(_get_services)):
    repaired = await svc.reconciler.reconcile()
    return {"repaired": repaired}


@app.get("/reconciler/history")
async def reconciler_history(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    action: str | None = Query(None),
    task_id: str | None = Query(None),
    svc: ServiceContainer = Depends(_get_services),
):
    items = svc.reconciler.list_repair_history(limit=limit, offset=offset, action=action, task_id=task_id)
    return {
        "items": items,
        "count": len(items),
        "filters": {"action": action, "task_id": task_id},
    }


@app.get("/capabilities")
async def list_capabilities(svc: ServiceContainer = Depends(_get_services)):
    return {
        "items": [item.model_dump() for item in svc.registry.list_capability_info()],
        "metrics": svc.registry.get_metrics(),
    }


@app.get("/capabilities/{capability_name}")
async def get_capability(capability_name: str, svc: ServiceContainer = Depends(_get_services)):
    info = svc.registry.get_info(capability_name)
    if info is None:
        raise HTTPException(status_code=404, detail={"error_code": "CAPABILITY_NOT_FOUND", "message": f"Capability {capability_name!r} not found"})
    return info.model_dump()


@app.post("/tenants", response_model=Tenant, status_code=201)
async def create_tenant(tenant: TenantCreate, svc: ServiceContainer = Depends(_get_services)):
    async with get_session() as session:
        created = await TenantRepository.create(session, tenant)
        if tenant.config:
            svc.quota_manager.configure_tenant(
                created.id,
                max_queued=tenant.config.get("max_queued"),
                max_running=tenant.config.get("max_running"),
            )
        return created


@app.get("/tenants", response_model=list[Tenant])
async def list_tenants(limit: int = Query(100, ge=1, le=1000)):
    async with get_session() as session:
        return await TenantRepository.list_all(session, limit=limit)


@app.get("/tenants/{tenant_id}", response_model=Tenant)
async def get_tenant(tenant_id: str):
    async with get_session() as session:
        tenant = await TenantRepository.get(session, tenant_id)
        if not tenant:
            raise HTTPException(status_code=404, detail={"error_code": "TENANT_NOT_FOUND", "message": "Tenant not found"})
        return tenant


@app.patch("/tenants/{tenant_id}", response_model=Tenant)
async def update_tenant(tenant_id: str, update: TenantUpdate, svc: ServiceContainer = Depends(_get_services)):
    async with get_session() as session:
        tenant = await TenantRepository.merge_config(session, tenant_id, update.config)
        if not tenant:
            raise HTTPException(status_code=404, detail={"error_code": "TENANT_NOT_FOUND", "message": "Tenant not found"})
        svc.quota_manager.configure_tenant(
            tenant_id,
            max_queued=tenant.config.get("max_queued"),
            max_running=tenant.config.get("max_running"),
        )
        return tenant


# ---------------------------------------------------------------------------
# P2 G9: Actor Pool endpoints
# ---------------------------------------------------------------------------

@app.get("/actors/capabilities")
async def list_actor_capabilities(svc: ServiceContainer = Depends(_get_services)):
    """List capabilities registered in the actor pool."""
    if svc.actor_pool_manager is None:
        raise HTTPException(status_code=404, detail="Actor pool not available")
    return {"capabilities": svc.actor_pool_manager.list_capabilities()}


@app.get("/actors/{capability}/stats")
async def get_actor_pool_stats(capability: str, svc: ServiceContainer = Depends(_get_services)):
    """Get stats for an actor pool capability."""
    if svc.actor_pool_manager is None:
        raise HTTPException(status_code=404, detail="Actor pool not available")
    return svc.actor_pool_manager.get_pool_stats(capability)


# ---------------------------------------------------------------------------
# P2 G10: Resource Manager endpoints
# ---------------------------------------------------------------------------

@app.get("/resources/stats")
async def get_resource_manager_stats(svc: ServiceContainer = Depends(_get_services)):
    """Get resource manager scaling stats."""
    if svc.resource_manager is None:
        raise HTTPException(status_code=404, detail="Resource manager not available")
    return svc.resource_manager.get_stats()


@app.get("/resources/scale-history")
async def get_scale_history(limit: int = Query(50, ge=1, le=500), svc: ServiceContainer = Depends(_get_services)):
    """Get recent scaling events."""
    if svc.resource_manager is None:
        raise HTTPException(status_code=404, detail="Resource manager not available")
    return {"events": svc.resource_manager.get_scale_history(limit)}


# ---------------------------------------------------------------------------
# P2 G11: Async Proxy Sidecar endpoints
# ---------------------------------------------------------------------------

@app.get("/async-proxy/stats")
async def get_async_proxy_stats(svc: ServiceContainer = Depends(_get_services)):
    """Get async proxy sidecar stats."""
    if svc.async_proxy_sidecar is None:
        raise HTTPException(status_code=404, detail="Async proxy not available")
    return svc.async_proxy_sidecar.get_stats()


# ---------------------------------------------------------------------------
# P2 G12: Cluster Registry endpoints
# ---------------------------------------------------------------------------

@app.get("/clusters")
async def list_clusters(svc: ServiceContainer = Depends(_get_services)):
    """List all registered clusters."""
    if svc.cluster_registry is None:
        raise HTTPException(status_code=404, detail="Cluster registry not available")
    clusters = await svc.cluster_registry.list_clusters()
    return {"clusters": [c.to_dict() for c in clusters]}


@app.get("/clusters/{cluster_id}")
async def get_cluster(cluster_id: str, svc: ServiceContainer = Depends(_get_services)):
    """Get a specific cluster by ID."""
    if svc.cluster_registry is None:
        raise HTTPException(status_code=404, detail="Cluster registry not available")
    cluster = await svc.cluster_registry.get_cluster(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_id!r} not found")
    return cluster.to_dict()


@app.get("/clusters/by-capability/{capability}")
async def get_clusters_for_capability(capability: str, svc: ServiceContainer = Depends(_get_services)):
    """Get all clusters that support a given capability."""
    if svc.cluster_registry is None:
        raise HTTPException(status_code=404, detail="Cluster registry not available")
    clusters = await svc.cluster_registry.get_clusters_for_capability(capability)
    return {"capability": capability, "clusters": [c.to_dict() for c in clusters]}


# ---------------------------------------------------------------------------
# Streaming: DAG execution progress via Server-Sent Events (SSE)
# ---------------------------------------------------------------------------
#
# GET /dags/{dag_id}/stream
#   Streams node status changes as SSE events while the DAG runs.
#   Each event is a JSON line: {"event": "node_update"|"dag_done", ...}
#
# POST /dags/stream
#   Create + execute a DAG and stream its progress in one call.
#
# Internal: _DagProgressBus — in-process asyncio.Queue per dag_id

_dag_progress_buses: dict[str, _Queue] = {}
_MAX_SSE_QUEUE = 256


def _get_or_create_bus(dag_id: str) -> _Queue:
    if dag_id not in _dag_progress_buses:
        _dag_progress_buses[dag_id] = _Queue(maxsize=_MAX_SSE_QUEUE)
    return _dag_progress_buses[dag_id]


def _push_event(dag_id: str, event: dict) -> None:
    """Non-blocking push to bus; drops if full (old events obsolete)."""
    bus = _dag_progress_buses.get(dag_id)
    if bus is not None:
        try:
            bus.put_nowait(event)
        except Exception:
            pass


async def _sse_generator(dag_id: str, timeout: float = 120.0):
    """Yield SSE-formatted lines from the dag progress bus."""
    bus = _get_or_create_bus(dag_id)
    deadline = time.monotonic() + timeout
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                yield 'data: {"event": "timeout"}\n\n'
                break
            try:
                event = await asyncio.wait_for(bus.get(), timeout=min(remaining, 5.0))
                yield f"data: {_json.dumps(event)}\n\n"
                if event.get("event") in ("dag_done", "dag_failed", "dag_cancelled"):
                    break
            except asyncio.TimeoutError:
                yield 'data: {"event": "heartbeat"}\n\n'
    finally:
        _dag_progress_buses.pop(dag_id, None)


def _make_progress_handler(dag_id: str, original_handler):
    """Wrap a handler to emit SSE progress events on each node call."""
    async def _wrapped(task_type: str, payload: dict):
        _push_event(dag_id, {
            "event": "node_start",
            "node": payload.get("__node_id__", task_type),
            "task_type": task_type,
        })
        try:
            result = await original_handler(task_type, payload)
            _push_event(dag_id, {
                "event": "node_done",
                "node": payload.get("__node_id__", task_type),
                "task_type": task_type,
                "result": result if isinstance(result, dict) else {"value": str(result)},
            })
            return result
        except Exception as exc:
            _push_event(dag_id, {
                "event": "node_error",
                "node": payload.get("__node_id__", task_type),
                "task_type": task_type,
                "error": str(exc),
            })
            raise
    return _wrapped


@app.get("/dags/{dag_id}/stream", response_class=StreamingResponse)
async def stream_dag_progress(dag_id: str, timeout: float = Query(120.0, ge=1.0, le=600.0)):
    """Stream DAG node progress as Server-Sent Events.

    Connect before or immediately after starting DAG execution.
    Events: node_start | node_done | node_error | dag_done | dag_failed | heartbeat | timeout
    """
    _get_or_create_bus(dag_id)  # ensure bus exists before DAG starts
    return StreamingResponse(
        _sse_generator(dag_id, timeout=timeout),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.post("/dags/stream", response_class=StreamingResponse)
async def create_and_stream_dag(dag_create: DAGCreate, svc: ServiceContainer = Depends(_get_services)):
    """Create a DAG, execute it, and stream node progress as SSE.

    Combines dag creation + execution + streaming in one endpoint.
    The response body is a text/event-stream of JSON lines.
    """
    dag = DAG(**dag_create.model_dump())
    dag_id = dag.id
    _get_or_create_bus(dag_id)

    # Annotate nodes with __node_id__ so wrapper can emit correct id
    for node in dag.nodes:
        node.payload["__node_id__"] = node.id

    async def _run_and_stream():
        async def _exec():
            try:
                handler = svc.task_handler
                wrapped = _make_progress_handler(dag_id, handler)
                result = await svc.dag_engine.execute(dag, wrapped)
                final_event = {
                    "event": "dag_done" if result.status.value == "success" else
                             ("dag_failed" if result.status.value == "failed" else "dag_cancelled"),
                    "dag_id": dag_id,
                    "status": result.status.value,
                    "node_statuses": {
                        nid: {
                            "status": ex.status.value,
                            "skipped": ex.skipped,
                            "error": ex.error_message,
                        }
                        for nid, ex in result.node_executions.items()
                    },
                }
                _push_event(dag_id, final_event)
            except Exception as exc:
                _push_event(dag_id, {"event": "dag_failed", "dag_id": dag_id, "error": str(exc)})

        asyncio.create_task(_exec())

        async for chunk in _sse_generator(dag_id, timeout=300.0):
            yield chunk

    return StreamingResponse(
        _run_and_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
