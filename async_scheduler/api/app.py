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
    Schedule,
    ScheduleCreate,
    ScheduleStatus,
    Task,
    TaskCreate,
    TaskStatus,
)
from async_scheduler.core.models import Tenant, TenantCreate, TenantUpdate
from async_scheduler.persistence import DAGRepository, get_session, ScheduleRepository, TaskRepository, TenantRepository
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
    return {
        "status": "healthy",
        "queue_size": services.queue_manager.get_queue_count() if services else 0,
        "scheduled_count": services.queue_manager.get_scheduled_count() if services else 0,
        "consumer_running": services.task_consumer.is_running() if services else False,
        "scheduler_running": services.cron_scheduler.is_running() if services else False,
        "reconciler_running": services.reconciler.is_running() if services else False,
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

    return {
        "queue_sizes": sizes,
        "total_queued": services.queue_manager.get_queue_count(),
        "scheduled_count": services.queue_manager.get_scheduled_count(),
        "running_tasks": services.task_executor.get_running_count(),
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
    return services.reconciler.stats()


@app.post("/reconciler/run")
async def run_reconciler_once():
    if not services:
        raise HTTPException(status_code=503, detail="Services not available")
    repaired = await services.reconciler.reconcile()
    return {"repaired": repaired}


@app.get("/capabilities")
async def list_capabilities():
    if not services:
        raise HTTPException(status_code=503, detail="Services not available")
    return {"items": [item.model_dump() for item in services.registry.list_capability_info()]}


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
