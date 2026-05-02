"""FastAPI task service for managing tasks, schedules, and DAGs."""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query
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

# Global service container
services: ServiceContainer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown."""
    # Startup
    global services

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


# Pydantic request/response models
class TaskResponse(Task):
    """Task response model."""
    pass


class ScheduleResponse(Schedule):
    """Schedule response model."""
    pass


class DAGResponse(DAG):
    """DAG response model."""
    pass


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


class ExecutionAttemptResponse(ExecutionAttempt):
    """Execution attempt response model."""
    pass


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
    """Health check endpoint."""
    worker_count = 0
    if services and services.worker_registry is not None:
        worker_count = len(await services.worker_registry.list_workers(include_stale=False))
    repair_history_count = 0
    if services:
        repair_history_count = len(services.reconciler.list_repair_history(limit=1000, offset=0))
    return {
        "status": "healthy",
        "queue_size": services.queue_manager.get_queue_count() if services else 0,
        "scheduled_count": services.queue_manager.get_scheduled_count() if services else 0,
        "consumer_running": services.task_consumer.is_running() if services else False,
        "scheduler_running": services.cron_scheduler.is_running() if services else False,
        "reconciler_running": services.reconciler.is_running() if services else False,
        "worker_count": worker_count,
        "repair_history_count": repair_history_count,
    }


# Task endpoints


@app.post("/tasks", response_model=TaskResponse, status_code=201)
async def create_task(task: TaskCreate):
    """Create a new task."""
    async with get_session() as session:
        if services is None:
            raise HTTPException(status_code=503, detail="Services not available")
        try:
            db_task = await services.task_router.create_task(task)
        except QuotaExceededError as e:
            raise HTTPException(status_code=429, detail=str(e)) from e
        return TaskResponse.model_validate(db_task)


@app.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(task_id: str):
    """Get a task by ID."""
    async with get_session() as session:
        task = await TaskRepository.get(session, task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
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
            raise HTTPException(status_code=404, detail="Task not found")
        attempts = await ExecutionAttemptRepository.list_for_task(session, task_id, limit=limit, offset=offset)
        return [ExecutionAttemptResponse.model_validate(attempt) for attempt in attempts]


@app.get("/tasks/{task_id}/attempts/latest", response_model=ExecutionAttemptResponse)
async def get_latest_attempt(task_id: str):
    """Get the latest execution attempt for a task."""
    async with get_session() as session:
        task = await TaskRepository.get(session, task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task_id)
        if not attempt:
            raise HTTPException(status_code=404, detail="Execution attempt not found")
        return ExecutionAttemptResponse.model_validate(attempt)


@app.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    """Cancel a task."""
    # Cancel from queue
    cancelled = False
    if services:
        cancelled = await services.queue_manager.cancel(task_id)
        cancelled = await services.task_executor.cancel(task_id) or cancelled

    # Update status in database
    async with get_session() as session:
        task = await TaskRepository.update(session, task_id, status=TaskStatus.CANCELLED)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

    return {"message": "Task cancelled", "task_id": task_id}


@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    """Delete a task."""
    async with get_session() as session:
        deleted = await TaskRepository.delete(session, task_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Task not found")
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
            raise HTTPException(status_code=404, detail="Schedule not found")
        return ScheduleResponse.model_validate(schedule)


@app.get("/schedules", response_model=list[ScheduleResponse])
async def list_schedules(limit: int = Query(100, ge=1, le=1000)):
    """List all schedules."""
    async with get_session() as session:
        schedules = await ScheduleRepository.list_active(session, limit=limit)
        return [ScheduleResponse.model_validate(s) for s in schedules]


@app.post("/schedules/{schedule_id}/trigger")
async def trigger_schedule(schedule_id: str):
    """Trigger a schedule execution immediately."""
    if not services:
        raise HTTPException(status_code=503, detail="Scheduler not available")

    triggered = await services.cron_scheduler.trigger_schedule(schedule_id)
    if not triggered:
        raise HTTPException(status_code=404, detail="Schedule not found or not active")

    return {"message": "Schedule triggered", "schedule_id": schedule_id}


@app.post("/schedules/{schedule_id}/pause", response_model=ScheduleResponse)
async def pause_schedule(schedule_id: str):
    from async_scheduler.scheduler import ScheduleRegistry

    registry = ScheduleRegistry()
    schedule = await registry.pause(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return ScheduleResponse.model_validate(schedule)


@app.post("/schedules/{schedule_id}/resume", response_model=ScheduleResponse)
async def resume_schedule(schedule_id: str):
    from async_scheduler.scheduler import ScheduleRegistry

    registry = ScheduleRegistry()
    schedule = await registry.resume(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
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
            raise HTTPException(status_code=404, detail="DAG not found")
        return DAGResponse.model_validate(dag)


@app.get("/dags", response_model=list[DAGResponse])
async def list_dags(limit: int = Query(100, ge=1, le=1000)):
    """List all DAGs."""
    from async_scheduler.persistence import DAGRepository

    async with get_session() as session:
        dags = await DAGRepository.list_all(session, limit=limit)
        return [DAGResponse.model_validate(d) for d in dags]


@app.post("/dags/execute", response_model=DAGExecuteResponse)
async def execute_dag(request: DAGExecuteRequest):
    """Execute a DAG."""
    if not services:
        raise HTTPException(status_code=503, detail="DAG engine not available")

    async with get_session() as session:
        dag = await DAGRepository.get(session, request.dag_id)
        if not dag:
            raise HTTPException(status_code=404, detail="DAG not found")

    async with get_session() as session:
        await DAGRepository.update(
            session,
            dag.id,
            status="running",
            started_at=datetime.utcnow(),
        )

    async def _execute():
        result_dag = await services.dag_engine.execute(dag, services.dag_handler)
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
async def cancel_dag(dag_id: str):
    """Cancel a running DAG execution."""
    if not services:
        raise HTTPException(status_code=503, detail="DAG engine not available")

    cancelled = await services.dag_engine.cancel(dag_id)
    if not cancelled:
        raise HTTPException(status_code=404, detail="DAG not found or not running")

    return {"message": "DAG cancelled", "dag_id": dag_id}


# Queue endpoints


@app.get("/queue/stats")
async def queue_stats():
    """Get queue statistics."""
    if not services:
        raise HTTPException(status_code=503, detail="Queue manager not available")

    sizes = await services.queue_manager.size()
    worker_count = 0
    if services.worker_registry is not None:
        worker_count = len(await services.worker_registry.list_workers(include_stale=False))

    return {
        "queue_sizes": sizes,
        "total_queued": services.queue_manager.get_queue_count(),
        "scheduled_count": services.queue_manager.get_scheduled_count(),
        "running_tasks": services.task_executor.get_running_count(),
        "worker_count": worker_count,
        "reconciler_running": services.reconciler.is_running(),
    }


@app.get("/debug/summary")
async def debug_summary():
    if not services:
        raise HTTPException(status_code=503, detail="Services not available")

    queue_sizes = await services.queue_manager.size()
    workers = []
    if services.worker_registry is not None:
        workers = await services.worker_registry.list_workers(include_stale=True)
    repair_metrics = services.reconciler.get_metrics()
    repair_history = services.reconciler.list_repair_history(limit=10, offset=0)

    lease_items = []
    async with await get_session_no_context() as session:
        latest_attempts = await ExecutionAttemptRepository.list_latest_attempts(session, limit=200, offset=0)
        for attempt in latest_attempts:
            task = await TaskRepository.get(session, attempt.task_id)
            lease = None
            if services.lock_backend is not None and hasattr(services.lock_backend, "describe_lock"):
                lease = await services.lock_backend.describe_lock(f"task:{attempt.task_id}")
            lease_items.append(
                {
                    "task_id": attempt.task_id,
                    "task_status": None if task is None else task.status,
                    "worker_id": attempt.worker_id,
                    "attempt_status": attempt.status,
                    "locked": False if lease is None else bool(lease.get("locked")),
                    "lease": lease,  # include for stale check
                }
            )

    locked_count = sum(1 for item in lease_items if item["locked"])
    running_without_lock_count = sum(
        1 for item in lease_items if item["task_status"] == TaskStatus.RUNNING and not item["locked"]
    )
    running_with_lock_count = sum(
        1 for item in lease_items if item["task_status"] == TaskStatus.RUNNING and item["locked"]
    )
    # 新统计：lease 锁着但 task 已经是终态（SUCCESS/FAILED/CANCELLED/TIMEOUT）
    locked_but_terminal_count = sum(
        1 for item in lease_items
        if item["locked"]
        and item["task_status"]
        and item["task_status"]
        in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT)
    )
    # 新统计：attempt ABANDONED 但 task 还是 RUNNING
    abandoned_but_running_count = sum(
        1 for item in lease_items
        if item["attempt_status"] == ExecutionAttemptStatus.ABANDONED
        and item["task_status"] == TaskStatus.RUNNING
    )
    # 新统计：stale lease（TTL < 5 秒）
    stale_lease_count = 0
    if services.lock_backend is not None and hasattr(services.lock_backend, "describe_lock"):
        # 采样检查前几个 locked 项的 TTL
        for item in lease_items[:10]:
            if item["locked"] and item["lease"]:
                ttl_ms = item["lease"].get("ttl_ms")
                if ttl_ms is not None and 0 < ttl_ms < 5000:
                    stale_lease_count += 1

    anomaly_summary = {
        "running_without_lock": running_without_lock_count,
        "locked_but_terminal": locked_but_terminal_count,
        "abandoned_but_running": abandoned_but_running_count,
        "stale_lease": stale_lease_count,
        "total": (
            running_without_lock_count
            + locked_but_terminal_count
            + abandoned_but_running_count
            + stale_lease_count
        ),
        "endpoint": "/debug/leases/anomalies",
        "summaryEndpoint": "/debug/leases/anomalies/summary",
    }

    return {
        "health": {
            "consumer_running": services.task_consumer.is_running(),
            "scheduler_running": services.cron_scheduler.is_running(),
            "reconciler_running": services.reconciler.is_running(),
        },
        "queue": {
            "sizes": queue_sizes,
            "total_queued": services.queue_manager.get_queue_count(),
            "scheduled_count": services.queue_manager.get_scheduled_count(),
            "running_tasks": services.task_executor.get_running_count(),
        },
        "workers": {
            "count": len(workers),
            "items": [worker.__dict__ for worker in workers],
        },
        "leases": {
            "count": len(lease_items),
            "locked_count": locked_count,
            "running_with_lock_count": running_with_lock_count,
            "running_without_lock_count": running_without_lock_count,
            "locked_but_terminal_count": locked_but_terminal_count,
            "abandoned_but_running_count": abandoned_but_running_count,
            "stale_lease_count": stale_lease_count,
            "anomaly_summary": anomaly_summary,
            "items": lease_items[:20],
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
    }


@app.get("/quota/stats")
async def quota_stats():
    if not services:
        raise HTTPException(status_code=503, detail="Services not available")
    return services.quota_manager.stats()


@app.get("/reconciler/stats")
async def reconciler_stats():
    if not services:
        raise HTTPException(status_code=503, detail="Services not available")
    metrics = services.reconciler.get_metrics()
    return {
        "total_runs": metrics.total_runs,
        "stuck_tasks_found": metrics.stuck_tasks_found,
        "tasks_repaired": metrics.tasks_repaired,
        "tasks_ignored": metrics.tasks_ignored,
        "tasks_requeued": metrics.tasks_requeued,
        "last_run_at": metrics.last_run_at,
        "last_repaired_count": metrics.last_repaired_count,
        "repair_rate": metrics.get_repair_rate(),
        "running": services.reconciler.is_running(),
    }


@app.get("/workers")
async def list_workers(include_stale: bool = Query(False)):
    if not services or services.worker_registry is None:
        raise HTTPException(status_code=503, detail="Worker registry not available")
    workers = await services.worker_registry.list_workers(include_stale=include_stale)
    return {
        "items": [worker.__dict__ for worker in workers],
        "count": len(workers),
        "include_stale": include_stale,
    }


@app.get("/workers/{worker_id}")
async def get_worker(worker_id: str):
    if not services or services.worker_registry is None:
        raise HTTPException(status_code=503, detail="Worker registry not available")
    workers = await services.worker_registry.list_workers(include_stale=True)
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
):
    if not services:
        raise HTTPException(status_code=503, detail="Services not available")

    async with await get_session_no_context() as session:
        attempts = await ExecutionAttemptRepository.list_latest_attempts(
            session,
            limit=limit,
            offset=offset,
            worker_id=worker_id,
        )
        items = []
        for attempt in attempts:
            task = await TaskRepository.get(session, attempt.task_id)
            lease = None
            if services.lock_backend is not None and hasattr(services.lock_backend, "describe_lock"):
                lease = await services.lock_backend.describe_lock(f"task:{attempt.task_id}")

            item = {
                "task_id": attempt.task_id,
                "task_status": None if task is None else task.status,
                "lease": lease,
                "latest_attempt": {
                    "id": attempt.id,
                    "worker_id": attempt.worker_id,
                    "retry_index": attempt.retry_index,
                    "status": attempt.status,
                    "lease_token": attempt.lease_token,
                    "started_at": attempt.started_at,
                    "last_heartbeat_at": attempt.last_heartbeat_at,
                    "completed_at": attempt.completed_at,
                },
            }

            if task_status is not None:
                if task is None:
                    continue
                if task.status.value != task_status:
                    continue
            if attempt_status is not None:
                if attempt.status.value != attempt_status:
                    continue
            if locked_only and not (lease and lease.get("locked")):
                continue

            items.append(item)

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
async def list_lease_anomalies(limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    if not services:
        raise HTTPException(status_code=503, detail="Services not available")

    async with await get_session_no_context() as session:
        attempts = await ExecutionAttemptRepository.list_latest_attempts(session, limit=limit, offset=offset)
        items = []
        for attempt in attempts:
            task = await TaskRepository.get(session, attempt.task_id)
            lease = None
            if services.lock_backend is not None and hasattr(services.lock_backend, "describe_lock"):
                lease = await services.lock_backend.describe_lock(f"task:{attempt.task_id}")

            task_status = None if task is None else task.status
            lease_locked = bool(lease and lease.get("locked"))
            anomaly_types: list[str] = []
            if task_status == TaskStatus.RUNNING and not lease_locked:
                anomaly_types.append("running_without_lock")
            if task_status in {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT} and lease_locked:
                anomaly_types.append("locked_but_terminal")
            if attempt.status == ExecutionAttemptStatus.ABANDONED and task_status == TaskStatus.RUNNING:
                anomaly_types.append("abandoned_but_running")
            ttl_ms = None if lease is None else lease.get("ttl_ms")
            if lease_locked and ttl_ms is not None and 0 < ttl_ms < 5000:
                anomaly_types.append("stale_lease")

            if anomaly_types:
                items.append(
                    {
                        "task_id": attempt.task_id,
                        "task_status": task_status,
                        "lease": lease,
                        "latest_attempt": {
                            "id": attempt.id,
                            "worker_id": attempt.worker_id,
                            "retry_index": attempt.retry_index,
                            "status": attempt.status,
                            "lease_token": attempt.lease_token,
                            "started_at": attempt.started_at,
                            "last_heartbeat_at": attempt.last_heartbeat_at,
                            "completed_at": attempt.completed_at,
                        },
                        "anomaly_types": anomaly_types,
                    }
                )

    return {"items": items, "count": len(items), "limit": limit, "offset": offset}


@app.get("/debug/leases/anomalies/summary")
async def get_lease_anomaly_summary(limit: int = Query(200, ge=1, le=1000), offset: int = Query(0, ge=0)):
    if not services:
        raise HTTPException(status_code=503, detail="Services not available")

    async with await get_session_no_context() as session:
        attempts = await ExecutionAttemptRepository.list_latest_attempts(session, limit=limit, offset=offset)
        by_type = {
            "running_without_lock": 0,
            "locked_but_terminal": 0,
            "abandoned_but_running": 0,
            "stale_lease": 0,
        }
        sample_items: dict[str, list[dict]] = {key: [] for key in by_type}
        for attempt in attempts:
            task = await TaskRepository.get(session, attempt.task_id)
            lease = None
            if services.lock_backend is not None and hasattr(services.lock_backend, "describe_lock"):
                lease = await services.lock_backend.describe_lock(f"task:{attempt.task_id}")

            task_status = None if task is None else task.status
            lease_locked = bool(lease and lease.get("locked"))
            anomaly_types: list[str] = []
            if task_status == TaskStatus.RUNNING and not lease_locked:
                anomaly_types.append("running_without_lock")
            if task_status in {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT} and lease_locked:
                anomaly_types.append("locked_but_terminal")
            if attempt.status == ExecutionAttemptStatus.ABANDONED and task_status == TaskStatus.RUNNING:
                anomaly_types.append("abandoned_but_running")
            ttl_ms = None if lease is None else lease.get("ttl_ms")
            if lease_locked and ttl_ms is not None and 0 < ttl_ms < 5000:
                anomaly_types.append("stale_lease")

            for anomaly in anomaly_types:
                by_type[anomaly] += 1
                if len(sample_items[anomaly]) < 5:
                    sample_items[anomaly].append(
                        {
                            "task_id": attempt.task_id,
                            "task_status": task_status,
                            "worker_id": attempt.worker_id,
                            "attempt_status": attempt.status,
                        }
                    )

    return {
        "counts": by_type,
        "samples": sample_items,
        "total": sum(by_type.values()),
        "sourceEndpoint": "/debug/leases/anomalies",
        "limit": limit,
        "offset": offset,
    }


@app.get("/debug/leases/{task_id}")
async def get_task_lease_debug(task_id: str):
    if not services:
        raise HTTPException(status_code=503, detail="Services not available")

    lease_info = None
    if services.lock_backend is not None and hasattr(services.lock_backend, "describe_lock"):
        lease_info = await services.lock_backend.describe_lock(f"task:{task_id}")

    latest_attempt = None
    async with await get_session_no_context() as session:
        task = await TaskRepository.get(session, task_id)
        if task is not None:
            latest_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task_id)

    if latest_attempt is None and task is None:
        raise HTTPException(status_code=404, detail="Task not found")

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
async def get_worker_leases(worker_id: str, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    if not services:
        raise HTTPException(status_code=503, detail="Services not available")

    async with await get_session_no_context() as session:
        attempts = await ExecutionAttemptRepository.list_latest_attempts(
            session,
            limit=limit,
            offset=offset,
            worker_id=worker_id,
        )
        items = []
        for attempt in attempts:
            task = await TaskRepository.get(session, attempt.task_id)
            lease = None
            if services.lock_backend is not None and hasattr(services.lock_backend, "describe_lock"):
                lease = await services.lock_backend.describe_lock(f"task:{attempt.task_id}")
            items.append(
                {
                    "task_id": attempt.task_id,
                    "task_status": None if task is None else task.status,
                    "lease": lease,
                    "latest_attempt": {
                        "id": attempt.id,
                        "worker_id": attempt.worker_id,
                        "retry_index": attempt.retry_index,
                        "status": attempt.status,
                        "lease_token": attempt.lease_token,
                        "started_at": attempt.started_at,
                        "last_heartbeat_at": attempt.last_heartbeat_at,
                        "completed_at": attempt.completed_at,
                    },
                }
            )

    return {
        "worker_id": worker_id,
        "items": items,
        "count": len(items),
        "limit": limit,
        "offset": offset,
    }


@app.post("/reconciler/run")
async def run_reconciler_once():
    if not services:
        raise HTTPException(status_code=503, detail="Services not available")
    repaired = await services.reconciler.reconcile()
    return {"repaired": repaired}


@app.get("/reconciler/history")
async def reconciler_history(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    action: str | None = Query(None),
    task_id: str | None = Query(None),
):
    if not services:
        raise HTTPException(status_code=503, detail="Services not available")
    items = services.reconciler.list_repair_history(
        limit=limit,
        offset=offset,
        action=action,
        task_id=task_id,
    )
    return {
        "items": items,
        "count": len(items),
        "filters": {"action": action, "task_id": task_id},
    }


@app.get("/capabilities")
async def list_capabilities():
    if not services:
        raise HTTPException(status_code=503, detail="Services not available")
    return {
        "items": [item.model_dump() for item in services.registry.list_capability_info()],
        "metrics": services.registry.get_metrics(),
    }


@app.get("/capabilities/{capability_name}")
async def get_capability(capability_name: str):
    if not services:
        raise HTTPException(status_code=503, detail="Services not available")
    info = services.registry.get_info(capability_name)
    if info is None:
        raise HTTPException(status_code=404, detail="Capability not found")
    return info.model_dump()


@app.post("/tenants", response_model=Tenant, status_code=201)
async def create_tenant(tenant: TenantCreate):
    async with get_session() as session:
        created = await TenantRepository.create(session, tenant)
        if services and tenant.config:
            services.quota_manager.configure_tenant(
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
            raise HTTPException(status_code=404, detail="Tenant not found")
        return tenant


@app.patch("/tenants/{tenant_id}", response_model=Tenant)
async def update_tenant(tenant_id: str, update: TenantUpdate):
    async with get_session() as session:
        tenant = await TenantRepository.merge_config(session, tenant_id, update.config)
        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")
        if services:
            services.quota_manager.configure_tenant(
                tenant_id,
                max_queued=tenant.config.get("max_queued"),
                max_running=tenant.config.get("max_running"),
            )
        return tenant
