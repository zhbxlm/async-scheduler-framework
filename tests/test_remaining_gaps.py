"""Tests for remaining gap-fill items: G3, G5, G6, G7."""
from __future__ import annotations

import asyncio
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# G3: TaskRouter idempotency_key + dispatch_mode + rollback
# ---------------------------------------------------------------------------

class TestTaskRouterIdempotency:
    """G3: idempotency_key prevents duplicate task creation."""

    @pytest.mark.asyncio
    async def test_idempotency_key_deduplication_in_process(self, tmp_path):
        """Second call with same idempotency_key returns existing task."""
        from async_scheduler.platform.router import TaskRouter, DispatchMode
        from async_scheduler.core.models import TaskCreate, TaskPriority

        # Build a real in-process service container (no Redis)
        from async_scheduler.platform.services import build_service_container
        services = await build_service_container()

        task_create = TaskCreate(
            name="idem-test",
            payload={"x": 1},
            idempotency_key="test-key-001",
        )

        t1 = await services.task_router.create_task(task_create)
        t2 = await services.task_router.create_task(task_create)

        assert t1.id == t2.id, "Idempotent calls must return same task"

    @pytest.mark.asyncio
    async def test_different_idempotency_keys_create_different_tasks(self):
        """Different keys create different tasks."""
        from async_scheduler.platform.services import build_service_container
        from async_scheduler.core.models import TaskCreate

        services = await build_service_container()

        t1 = await services.task_router.create_task(TaskCreate(
            name="idem-a", payload={}, idempotency_key="key-A",
        ))
        t2 = await services.task_router.create_task(TaskCreate(
            name="idem-b", payload={}, idempotency_key="key-B",
        ))
        assert t1.id != t2.id

    @pytest.mark.asyncio
    async def test_dispatch_mode_direct(self):
        """DispatchMode.DIRECT creates and enqueues task normally."""
        from async_scheduler.platform.services import build_service_container
        from async_scheduler.core.models import TaskCreate
        from async_scheduler.platform.router import DispatchMode

        services = await build_service_container()
        task = await services.task_router.create_task(
            TaskCreate(name="direct-task", payload={}),
            dispatch_mode=DispatchMode.DIRECT,
        )
        assert task.id is not None

    @pytest.mark.asyncio
    async def test_dispatch_mode_dag_orchestrated(self):
        """DispatchMode.DAG_ORCHESTRATED routes through queue normally."""
        from async_scheduler.platform.services import build_service_container
        from async_scheduler.core.models import TaskCreate
        from async_scheduler.platform.router import DispatchMode

        services = await build_service_container()
        task = await services.task_router.create_task(
            TaskCreate(name="dag-task", payload={}, tags=["capability:dag"]),
            dispatch_mode=DispatchMode.DAG_ORCHESTRATED,
        )
        assert task.id is not None

    @pytest.mark.asyncio
    async def test_dispatch_mode_raydata_native(self):
        """DispatchMode.RAYDATA_NATIVE routes through capability queue."""
        from async_scheduler.platform.services import build_service_container
        from async_scheduler.core.models import TaskCreate
        from async_scheduler.platform.router import DispatchMode

        services = await build_service_container()
        task = await services.task_router.create_task(
            TaskCreate(name="ray-task", payload={}, tags=["capability:ml"]),
            dispatch_mode=DispatchMode.RAYDATA_NATIVE,
        )
        assert task.id is not None

    @pytest.mark.asyncio
    async def test_rollback_on_enqueue_failure(self):
        """On enqueue failure, DB record is deleted and idem_key released."""
        from async_scheduler.platform.services import build_service_container
        from async_scheduler.core.models import TaskCreate
        from async_scheduler.platform.router import DispatchMode

        services = await build_service_container()

        # Patch queue_manager.enqueue to raise
        services.task_router.queue_manager.enqueue = AsyncMock(
            side_effect=RuntimeError("queue unavailable")
        )

        with pytest.raises(RuntimeError, match="rolled back"):
            await services.task_router.create_task(
                TaskCreate(name="fail-task", payload={}, idempotency_key="fail-key"),
            )

        # After rollback, the idem key should be released → new call should work
        services.task_router.queue_manager.enqueue = AsyncMock(return_value=None)
        t2 = await services.task_router.create_task(
            TaskCreate(name="retry-task", payload={}, idempotency_key="fail-key"),
        )
        assert t2.id is not None

    def test_dispatch_mode_enum_values(self):
        from async_scheduler.platform.router import DispatchMode
        assert DispatchMode.DIRECT == "direct"
        assert DispatchMode.DAG_ORCHESTRATED == "dag_orchestrated"
        assert DispatchMode.RAYDATA_NATIVE == "raydata_native"


# ---------------------------------------------------------------------------
# G5: DAGEngine capability concurrency slot
# ---------------------------------------------------------------------------

class TestDAGEngineCapabilitySlot:
    """G5: DAGEngine acquires/releases capability concurrency slot per node."""

    def _make_dag(self):
        from async_scheduler.core.models import DAG, DAGNode
        node = DAGNode(
            id="n1",
            name="test-node",
            task_type="compute",
            payload={"capability": "gpu", "value": 1},
            dependencies=[],
        )
        return DAG(id="d1", name="test-dag", nodes=[node])

    @pytest.mark.asyncio
    async def test_dag_engine_accepts_queue_manager(self):
        """DAGEngine can be initialized with a queue_manager."""
        from async_scheduler.dag.engine import DAGEngine

        mock_qm = MagicMock()
        engine = DAGEngine(queue_manager=mock_qm)
        assert engine._queue_manager is mock_qm

    @pytest.mark.asyncio
    async def test_dag_engine_calls_acquire_and_release(self):
        """_acquire_capability_slot and _release_capability_slot are called."""
        from async_scheduler.dag.engine import DAGEngine

        mock_qm = MagicMock()
        mock_qm.acquire_concurrency_slot = AsyncMock(return_value=True)
        mock_qm.record_result = AsyncMock()
        mock_qm.try_recover_concurrent = AsyncMock()

        engine = DAGEngine(queue_manager=mock_qm)

        dag = self._make_dag()
        handler = AsyncMock(return_value={"result": "ok"})

        await engine.execute(dag, handler)

        mock_qm.acquire_concurrency_slot.assert_called_once_with("gpu")
        mock_qm.record_result.assert_called()
        mock_qm.try_recover_concurrent.assert_called()

    @pytest.mark.asyncio
    async def test_dag_engine_slot_released_on_failure(self):
        """Even if node fails, slot is released via finally."""
        from async_scheduler.dag.engine import DAGEngine

        mock_qm = MagicMock()
        mock_qm.acquire_concurrency_slot = AsyncMock(return_value=True)
        mock_qm.record_result = AsyncMock()
        mock_qm.try_recover_concurrent = AsyncMock()

        engine = DAGEngine(queue_manager=mock_qm)
        dag = self._make_dag()
        handler = AsyncMock(side_effect=RuntimeError("node crash"))

        await engine.execute(dag, handler)  # should not raise

        # record_result should have been called
        assert mock_qm.record_result.called
        # first call's first positional arg should be capability name
        call_args = mock_qm.record_result.call_args_list[0]
        assert call_args[0][0] == "gpu"  # capability
        # second arg is success=False since handler raised
        assert call_args[0][1] is False

    @pytest.mark.asyncio
    async def test_dag_engine_no_queue_manager_still_works(self):
        """Without queue_manager, DAGEngine works normally (G5 is no-op)."""
        from async_scheduler.dag.engine import DAGEngine

        engine = DAGEngine()
        dag = self._make_dag()
        handler = AsyncMock(return_value={"result": "ok"})

        result = await engine.execute(dag, handler)
        from async_scheduler.core.models import DAGExecutionStatus
        assert result.status == DAGExecutionStatus.SUCCESS


# ---------------------------------------------------------------------------
# G6: DELAYED_PROMOTE_CAP_SCRIPT exists and is non-empty
# ---------------------------------------------------------------------------

class TestDelayedPromoteCapScript:
    """G6: Lua time-gated promotion script is present and well-formed."""

    def test_script_exists_and_non_empty(self):
        from async_scheduler.backends.redis import DELAYED_PROMOTE_CAP_SCRIPT
        assert len(DELAYED_PROMOTE_CAP_SCRIPT) > 100

    def test_script_contains_zrangebyscore(self):
        from async_scheduler.backends.redis import DELAYED_PROMOTE_CAP_SCRIPT
        assert "ZRANGEBYSCORE" in DELAYED_PROMOTE_CAP_SCRIPT.upper()

    def test_script_contains_zadd(self):
        from async_scheduler.backends.redis import DELAYED_PROMOTE_CAP_SCRIPT
        assert "ZADD" in DELAYED_PROMOTE_CAP_SCRIPT.upper()

    def test_script_contains_zrem(self):
        from async_scheduler.backends.redis import DELAYED_PROMOTE_CAP_SCRIPT
        assert "ZREM" in DELAYED_PROMOTE_CAP_SCRIPT.upper()

    def test_script_uses_namespace_arg(self):
        """Script should use ARGV[4] for namespace prefix."""
        from async_scheduler.backends.redis import DELAYED_PROMOTE_CAP_SCRIPT
        assert "ARGV[4]" in DELAYED_PROMOTE_CAP_SCRIPT

    def test_promote_due_tasks_uses_lua_path(self):
        """_promote_due_tasks calls eval(DELAYED_PROMOTE_CAP_SCRIPT) when Redis present."""
        from async_scheduler.backends.redis import RedisQueueBackend, DELAYED_PROMOTE_CAP_SCRIPT
        import inspect
        src = inspect.getsource(RedisQueueBackend._promote_due_tasks)
        assert "DELAYED_PROMOTE_CAP_SCRIPT" in src


# ---------------------------------------------------------------------------
# G7: TaskStatus.SCHEDULED set on future scheduled_at
# ---------------------------------------------------------------------------

class TestTaskRouterScheduledStatus:
    """G7: create_task sets status=SCHEDULED for future tasks."""

    @pytest.mark.asyncio
    async def test_future_task_gets_scheduled_status(self):
        """A task with scheduled_at in the future should be SCHEDULED in DB."""
        from async_scheduler.platform.services import build_service_container
        from async_scheduler.core.models import TaskCreate, TaskStatus
        from datetime import timezone

        services = await build_service_container()

        future = datetime.utcnow() + timedelta(hours=1)
        task = await services.task_router.create_task(TaskCreate(
            name="future-task",
            payload={},
            scheduled_at=future,
        ))

        assert task.status == TaskStatus.SCHEDULED, (
            f"Expected SCHEDULED, got {task.status}"
        )

    @pytest.mark.asyncio
    async def test_immediate_task_gets_queued_status(self):
        """A task with no scheduled_at should be QUEUED immediately."""
        from async_scheduler.platform.services import build_service_container
        from async_scheduler.core.models import TaskCreate, TaskStatus

        services = await build_service_container()

        task = await services.task_router.create_task(TaskCreate(
            name="immediate-task",
            payload={},
        ))

        assert task.status == TaskStatus.QUEUED

    @pytest.mark.asyncio
    async def test_past_scheduled_at_gets_queued_status(self):
        """A task with scheduled_at in the past should be QUEUED immediately."""
        from async_scheduler.platform.services import build_service_container
        from async_scheduler.core.models import TaskCreate, TaskStatus

        services = await build_service_container()

        past = datetime.utcnow() - timedelta(minutes=5)
        task = await services.task_router.create_task(TaskCreate(
            name="past-task",
            payload={},
            scheduled_at=past,
        ))

        assert task.status == TaskStatus.QUEUED
