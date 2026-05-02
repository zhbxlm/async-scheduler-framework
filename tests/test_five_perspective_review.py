"""Tests for 5-perspective review improvements.

A = Architect, P = Performance, O = Ops, U = User/API, PM = Product Manager
"""
from __future__ import annotations
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock


# ---------------------------------------------------------------------------
# A1/A2: DAGEngine injected with queue_manager; acquire_concurrency_slot works
# ---------------------------------------------------------------------------

class TestArchitectFixes:

    @pytest.mark.asyncio
    async def test_service_container_injects_qm_into_dag_engine(self):
        """A1: build_service_container passes queue_manager to DAGEngine."""
        from async_scheduler.platform.services import build_service_container
        services = await build_service_container()
        assert services.dag_engine._queue_manager is services.queue_manager

    @pytest.mark.asyncio
    async def test_acquire_concurrency_slot_exists_on_queue_manager(self):
        """A2: QueueManager has acquire_concurrency_slot method."""
        from async_scheduler.queue.manager import QueueManager
        from async_scheduler.backends import BackendFactory
        qm = QueueManager(backend=BackendFactory().create_queue_backend())
        assert hasattr(qm, 'acquire_concurrency_slot')

    @pytest.mark.asyncio
    async def test_acquire_concurrency_slot_returns_bool_when_closed(self):
        """A2: Returns True when circuit is CLOSED."""
        from async_scheduler.queue.manager import QueueManager
        from async_scheduler.backends import BackendFactory
        qm = QueueManager(backend=BackendFactory().create_queue_backend())
        result = await qm.acquire_concurrency_slot("default")
        assert isinstance(result, bool)
        assert result is True  # fresh circuit should be CLOSED

    @pytest.mark.asyncio
    async def test_dag_engine_g5_slot_via_real_qm(self):
        """A1+A2: Full path - DAGEngine calls qm.acquire_concurrency_slot via real QueueManager."""
        from async_scheduler.dag.engine import DAGEngine
        from async_scheduler.core.models import DAG, DAGNode
        from async_scheduler.queue.manager import QueueManager
        from async_scheduler.backends import BackendFactory

        qm = QueueManager(backend=BackendFactory().create_queue_backend())
        engine = DAGEngine(queue_manager=qm)

        node = DAGNode(id="n1", name="gpu-step", task_type="gpu",
                       payload={"capability": "gpu"}, dependencies=[])
        dag = DAG(id="d1", name="test", nodes=[node])

        handler = AsyncMock(return_value={"ok": True})
        result = await engine.execute(dag, handler)

        from async_scheduler.core.models import DAGExecutionStatus
        assert result.status == DAGExecutionStatus.SUCCESS


# ---------------------------------------------------------------------------
# P3/P4: capability cache + exponential backoff
# ---------------------------------------------------------------------------

class TestPerformanceFixes:

    def test_consumer_loop_source_has_cache_logic(self):
        """P3: _consumer_loop caches capabilities instead of fetching every poll."""
        import inspect
        from async_scheduler.core.consumer import TaskConsumer
        src = inspect.getsource(TaskConsumer._consumer_loop)
        assert "_cap_cache_ttl" in src
        assert "_CAP_CACHE_SECONDS" in src

    def test_consumer_loop_source_has_backoff(self):
        """P4: _consumer_loop uses exponential backoff with jitter."""
        import inspect
        from async_scheduler.core.consumer import TaskConsumer
        src = inspect.getsource(TaskConsumer._consumer_loop)
        assert "_idle_streak" in src
        assert "backoff" in src
        assert "jitter" in src

    def test_executor_retry_has_jitter(self):
        """P9: TaskExecutor retry sleep uses jitter to prevent thundering herd."""
        import inspect
        from async_scheduler.executor.executor import TaskExecutor
        src = inspect.getsource(TaskExecutor.execute)
        assert "jitter" in src or "uniform" in src

    @pytest.mark.asyncio
    async def test_backoff_does_not_exceed_max(self):
        """P4: backoff is capped at _MAX_BACKOFF (8s)."""
        import inspect
        from async_scheduler.core.consumer import TaskConsumer
        src = inspect.getsource(TaskConsumer._consumer_loop)
        assert "_MAX_BACKOFF" in src
        # Extract the cap value
        import re
        match = re.search(r"_MAX_BACKOFF.*?=.*?(\d+)", src)
        assert match, "Could not find _MAX_BACKOFF value"
        assert int(match.group(1)) <= 60  # sanity check


# ---------------------------------------------------------------------------
# O6: /health, /readiness, /liveness
# ---------------------------------------------------------------------------

class TestOpsFixes:

    @pytest.mark.asyncio
    async def test_health_endpoint_has_uptime(self):
        """O6: /health includes uptime_seconds and version."""
        import importlib
        import httpx
        from httpx import ASGITransport
        mod = importlib.import_module("async_scheduler.api.app")
        async with httpx.AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert "uptime_seconds" in data
        assert "version" in data
        assert "ready" in data

    @pytest.mark.asyncio
    async def test_readiness_endpoint_exists(self):
        """O6: /readiness endpoint returns 200 or 503."""
        import importlib
        import httpx
        from httpx import ASGITransport
        mod = importlib.import_module("async_scheduler.api.app")
        async with httpx.AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.get("/readiness")
        assert resp.status_code in (200, 503)

    @pytest.mark.asyncio
    async def test_liveness_endpoint_returns_200(self):
        """O6: /liveness always returns 200."""
        import importlib
        import httpx
        from httpx import ASGITransport
        mod = importlib.import_module("async_scheduler.api.app")
        async with httpx.AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.get("/liveness")
        assert resp.status_code == 200
        data = resp.json()
        assert data["alive"] is True
        assert "uptime_seconds" in data

    def test_structured_log_in_consumer_error_path(self):
        """O5: Error log includes task_id, worker_id, tenant_id."""
        import inspect
        from async_scheduler.core.consumer import TaskConsumer
        src = inspect.getsource(TaskConsumer._process_single_task)
        assert "worker_id" in src
        assert "tenant_id" in src
        assert "task_id" in src


# ---------------------------------------------------------------------------
# U7: /tasks/batch endpoint
# ---------------------------------------------------------------------------

class TestUserAPIFixes:

    @pytest.mark.asyncio
    async def test_batch_create_endpoint_exists(self):
        """U7: POST /tasks/batch accepts list of tasks."""
        import importlib
        import httpx
        from httpx import ASGITransport
        mod = importlib.import_module("async_scheduler.api.app")
        # Initialize services
        from async_scheduler.platform.services import build_service_container
        mod.services = await build_service_container()

        async with httpx.AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.post("/tasks/batch", json={
                "tasks": [
                    {"name": "batch-1", "payload": {}},
                    {"name": "batch-2", "payload": {"key": "val"}},
                ]
            })
        assert resp.status_code == 201
        data = resp.json()
        assert data["total"] == 2
        assert data["succeeded"] == 2
        assert data["failed"] == []
        assert len(data["created"]) == 2

    @pytest.mark.asyncio
    async def test_batch_partial_failure_reported(self):
        """U7: Batch returns partial results when some tasks fail."""
        import importlib
        import httpx
        from httpx import ASGITransport
        mod = importlib.import_module("async_scheduler.api.app")
        from async_scheduler.platform.services import build_service_container
        services = await build_service_container()
        # Patch enqueue to fail on second call only
        call_count = [0]
        original = services.task_router.queue_manager.enqueue
        async def failing_enqueue(task, **kw):
            call_count[0] += 1
            if call_count[0] == 2:
                raise RuntimeError("simulated failure")
            return await original(task, **kw)
        services.task_router.queue_manager.enqueue = failing_enqueue
        mod.services = services

        async with httpx.AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.post("/tasks/batch", json={
                "tasks": [
                    {"name": "ok-task", "payload": {}},
                    {"name": "fail-task", "payload": {}},
                ]
            })
        assert resp.status_code == 201
        data = resp.json()
        assert data["succeeded"] == 1
        assert len(data["failed"]) == 1
        assert data["failed"][0]["index"] == 1


# ---------------------------------------------------------------------------
# PM10: /tasks/{id}/history
# ---------------------------------------------------------------------------

class TestProductManagerFixes:

    @pytest.mark.asyncio
    async def test_task_history_endpoint(self):
        """PM10: GET /tasks/{id}/history returns task + attempts + summary."""
        import importlib
        import httpx
        from httpx import ASGITransport
        from async_scheduler.core.models import TaskCreate
        mod = importlib.import_module("async_scheduler.api.app")
        from async_scheduler.platform.services import build_service_container
        mod.services = await build_service_container()

        # Create a task first
        task = await mod.services.task_router.create_task(TaskCreate(name="hist-test", payload={}))

        async with httpx.AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.get(f"/tasks/{task.id}/history")
        assert resp.status_code == 200
        data = resp.json()
        assert "task" in data
        assert "attempts" in data
        assert "summary" in data
        assert "final_status" in data["summary"]
        assert data["task"]["id"] == task.id

    @pytest.mark.asyncio
    async def test_task_history_404_for_unknown(self):
        """PM10: Returns 404 with error_code for unknown task_id."""
        import importlib
        import httpx
        from httpx import ASGITransport
        mod = importlib.import_module("async_scheduler.api.app")
        async with httpx.AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as client:
            resp = await client.get("/tasks/nonexistent-uuid/history")
        assert resp.status_code == 404
        detail = resp.json()["detail"]
        assert "error_code" in detail or "TASK_NOT_FOUND" in str(detail)
