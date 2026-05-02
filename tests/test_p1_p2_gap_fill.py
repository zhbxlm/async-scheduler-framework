from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from async_scheduler.backends.memory import InMemoryQueueBackend
from async_scheduler.core.models import Task, TaskStatus, TaskPriority
from async_scheduler.platform.reconciler import ReconciliationConfig, TaskReconciler
from async_scheduler.queue.manager import QueueManager
from async_scheduler.scheduler.cron import CronScheduler


def _make_task(task_type: str = "default") -> Task:
    return Task(
        id=f"task-{task_type}-{id(object())}",
        name=f"test-{task_type}",
        task_type=task_type,
        priority=TaskPriority.NORMAL,
        payload={},
        status=TaskStatus.QUEUED,
        tenant_id="tenant-1",
        retry_count=0,
        max_retries=0,
        timeout_seconds=30,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


# ---------------------------------------------------------------------------
# P1-TODO-5: QueueManager passes capability to backend
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_queue_manager_passes_capability_to_backend():
    """QueueManager.dequeue forwards capability to the backend."""
    backend = InMemoryQueueBackend()
    qm = QueueManager(backend=backend)

    t_gpu = _make_task("gpu")
    t_cpu = _make_task("cpu")
    await backend.enqueue(t_gpu, capability="gpu")
    await backend.enqueue(t_cpu, capability="cpu")

    got_gpu = await qm.dequeue(capability="gpu")
    got_cpu = await qm.dequeue(capability="cpu")

    assert got_gpu is not None and got_gpu.id == t_gpu.id
    assert got_cpu is not None and got_cpu.id == t_cpu.id


@pytest.mark.asyncio
async def test_queue_manager_default_capability_fallback():
    """QueueManager.dequeue with no capability uses 'default'."""
    backend = InMemoryQueueBackend()
    qm = QueueManager(backend=backend)

    task = _make_task("default")
    await backend.enqueue(task, capability="default")

    got = await qm.dequeue()
    assert got is not None and got.id == task.id


# ---------------------------------------------------------------------------
# P1-TODO-6: TaskRouter infers capability from task_type
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_task_router_infers_capability():
    """TaskRouter.create_task enqueues to the capability from tags."""
    backend = InMemoryQueueBackend()
    qm = QueueManager(backend=backend)

    enqueued_capabilities = []
    original_enqueue = qm.enqueue

    async def tracking_enqueue(t, scheduled_at=None, capability="default"):
        enqueued_capabilities.append(capability)
        await backend.enqueue(t, capability=capability)

    qm.enqueue = tracking_enqueue

    from async_scheduler.platform.router import TaskRouter
    from async_scheduler.core.models import TaskCreate

    router = TaskRouter(queue_manager=qm)

    # Mock the DB operations
    with patch("async_scheduler.platform.router.get_session_no_context") as mock_ctx, \
         patch("async_scheduler.platform.router.TaskRepository") as mock_repo:
        mock_session = AsyncMock()
        mock_ctx.return_value = mock_session
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        task_create = TaskCreate(
            name="ml-job",
            payload={},
            tenant_id="t1",
            tags=["capability:ml-inference"],  # capability tag format
        )
        created_task = Task(
            id="task-ml-1",
            name="ml-job",
            priority=TaskPriority.NORMAL,
            payload={},
            status=TaskStatus.QUEUED,
            tenant_id="t1",
            retry_count=0,
            max_retries=0,
            timeout_seconds=30,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            tags=["capability:ml-inference"],
        )
        mock_repo.create = AsyncMock(return_value=created_task)
        mock_repo.update = AsyncMock(return_value=created_task)

        result = await router.create_task(task_create)

    # The capability should be derived from tags["capability"]
    assert len(enqueued_capabilities) == 1
    assert enqueued_capabilities[0] == "ml-inference"


# ---------------------------------------------------------------------------
# P1-TODO-7: CronScheduler leader lease renewal
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cron_scheduler_renewal_loop_renews_when_leader():
    """When is_leader=True, _renew_leader_lease is called and keeps leadership."""
    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=True)  # renewal succeeds
    mock_redis.get = AsyncMock(return_value="test-id")

    qm = MagicMock()
    scheduler = CronScheduler(
        queue_manager=qm,
        redis_client=mock_redis,
        leader_lease_ttl=9,
        instance_id="test-id",
    )
    scheduler._is_leader = True

    renewed = await scheduler._renew_leader_lease()
    assert renewed is True
    assert scheduler._is_leader is True
    mock_redis.set.assert_called_once()


@pytest.mark.asyncio
async def test_cron_scheduler_renewal_loop_loses_leadership():
    """When SET XX fails (key expired/stolen), is_leader becomes False."""
    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=None)  # renewal fails - key gone

    qm = MagicMock()
    scheduler = CronScheduler(
        queue_manager=qm,
        redis_client=mock_redis,
        leader_lease_ttl=9,
        instance_id="test-id",
    )
    scheduler._is_leader = True

    renewed = await scheduler._renew_leader_lease()
    assert renewed is False
    assert scheduler._is_leader is False


@pytest.mark.asyncio
async def test_cron_scheduler_renewal_is_no_op_without_redis():
    """Without Redis, _renew_leader_lease always returns True."""
    qm = MagicMock()
    scheduler = CronScheduler(queue_manager=qm, redis_client=None)
    scheduler._is_leader = True

    assert await scheduler._renew_leader_lease() is True
    assert scheduler._is_leader is True


# ---------------------------------------------------------------------------
# P1-TODO-8: TaskReconciler stale_ttl_seconds configurable
# ---------------------------------------------------------------------------

def test_reconciler_stale_ttl_default():
    """Default stale TTL is 3600 seconds."""
    reconciler = TaskReconciler()
    assert reconciler.config.stuck_after_seconds == 3600


def test_reconciler_stale_ttl_configurable():
    """stale_ttl_seconds parameter overrides config.stuck_after_seconds."""
    reconciler = TaskReconciler(stale_ttl_seconds=300)
    assert reconciler.config.stuck_after_seconds == 300


def test_reconciler_stale_ttl_via_config():
    """ReconciliationConfig.stuck_after_seconds can be set directly."""
    config = ReconciliationConfig(stuck_after_seconds=120)
    reconciler = TaskReconciler(config=config)
    assert reconciler.config.stuck_after_seconds == 120


def test_reconciler_stale_ttl_overrides_config():
    """stale_ttl_seconds parameter overrides even explicit config."""
    config = ReconciliationConfig(stuck_after_seconds=999)
    reconciler = TaskReconciler(config=config, stale_ttl_seconds=60)
    assert reconciler.config.stuck_after_seconds == 60


# ---------------------------------------------------------------------------
# P2-TODO-9: API endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_list_capabilities(tmp_path):
    """GET /queues/capabilities returns known capability names."""
    import importlib
    _mod = importlib.import_module("async_scheduler.api.app")

    mock_qm = AsyncMock()
    mock_qm.discover_capabilities = AsyncMock(return_value=["gpu", "cpu"])

    mock_services = MagicMock()
    mock_services.queue_manager = mock_qm

    original_services = _mod.services
    _mod.services = mock_services

    try:
        async with AsyncClient(transport=ASGITransport(app=_mod.app), base_url="http://test") as client:
            resp = await client.get("/queues/capabilities")
            assert resp.status_code == 200
            data = resp.json()
            assert "capabilities" in data
            assert set(data["capabilities"]) == {"gpu", "cpu"}
    finally:
        _mod.services = original_services


@pytest.mark.asyncio
async def test_api_capability_stats(tmp_path):
    """GET /queues/{capability}/stats returns stats for the capability."""
    import importlib
    _mod = importlib.import_module("async_scheduler.api.app")
    from async_scheduler.queue.manager import CapabilityQueueStats

    stats_obj = CapabilityQueueStats(
        capability="gpu",
        pending=5,
        running=2,
        max_concurrent=8,
        circuit_state="closed",
    )

    mock_qm = AsyncMock()
    mock_qm.get_capability_stats = AsyncMock(return_value=stats_obj)

    mock_services = MagicMock()
    mock_services.queue_manager = mock_qm

    original_services = _mod.services
    _mod.services = mock_services

    try:
        async with AsyncClient(transport=ASGITransport(app=_mod.app), base_url="http://test") as client:
            resp = await client.get("/queues/gpu/stats")
            assert resp.status_code == 200
            data = resp.json()
            assert data["capability"] == "gpu"
            assert data["pending"] == 5
            assert data["running"] == 2
    finally:
        _mod.services = original_services
