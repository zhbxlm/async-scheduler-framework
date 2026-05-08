"""Tests for TaskExecutor, TaskConsumer, CronScheduler, DagEngine (src/)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

# ===========================================================================
# TaskExecutor
# ===========================================================================

class TestTaskExecutor:
    def _make_redis(self):
        r = AsyncMock()
        r.set = AsyncMock(return_value=True)
        r.get = AsyncMock(return_value=None)
        r.delete = AsyncMock()
        r.eval = AsyncMock(return_value=b"ok")
        r.register_script = MagicMock(return_value=AsyncMock(return_value=b"ok"))
        return r

    @pytest.mark.asyncio
    async def test_execute_success(self):
        from src.platform.task_executor import TaskExecutor
        r = self._make_redis()
        callback_calls = []
        async def cb(task_id, result):
            callback_calls.append((task_id, result))

        executor = TaskExecutor(r, callback_fn=cb)
        result = await executor.execute("t1", None, {})
        assert result["status"] == "completed"
        assert callback_calls[0][0] == "t1"

    @pytest.mark.asyncio
    async def test_execute_cancelled_if_cancel_key_set(self):
        from src.platform.task_executor import TaskExecutor
        r = self._make_redis()
        r.get = AsyncMock(return_value=b"1")  # cancel flag set
        executor = TaskExecutor(r)
        result = await executor.execute("t1", None, {})
        assert result["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_execute_lock_fail_raises(self):
        from src.platform.task_executor import TaskExecutor
        r = self._make_redis()
        r.set = AsyncMock(return_value=None)  # NX fail = lock held
        executor = TaskExecutor(r)
        with pytest.raises(RuntimeError, match="failed to acquire lock"):
            await executor.execute("t1", None, {})


# ===========================================================================
# DagEngine (src/)
# ===========================================================================

class TestDagEngine:
    @pytest.mark.asyncio
    async def test_simple_linear_dag(self):
        from src.models.dag import DagDefinition, DagStep
        from src.platform.dag_engine import DagEngine

        calls = []
        async def dispatch(capability, step_name, payload):
            calls.append(step_name)
            return {"done": step_name}

        dag = DagDefinition(
            dag_id="d1",
            steps=[
                DagStep(step_name="a", capability="x"),
                DagStep(step_name="b", capability="x", depends_on=["a"]),
            ],
        )
        engine = DagEngine()
        ctx = await engine.execute(dag, dispatch)
        assert ctx.status.value == "completed"
        assert "a" in ctx.completed_steps
        assert "b" in ctx.completed_steps
        assert calls.index("a") < calls.index("b")

    @pytest.mark.asyncio
    async def test_dag_with_condition_skip(self):
        from src.models.dag import DagDefinition, DagStep
        from src.platform.dag_engine import DagEngine

        calls = []
        async def dispatch(capability, step_name, payload):
            calls.append(step_name)
            return {}

        dag = DagDefinition(
            dag_id="d2",
            steps=[
                DagStep(step_name="always", capability="x"),
                DagStep(step_name="conditional", capability="x", condition="False"),
            ],
        )
        engine = DagEngine()
        await engine.execute(dag, dispatch, initial_context={})
        assert "always" in calls
        assert "conditional" not in calls

    @pytest.mark.asyncio
    async def test_dag_fallback_on_failure(self):
        from src.models.dag import DagDefinition, DagStep, FallbackConfig, OnFailureAction
        from src.platform.dag_engine import DagEngine

        async def dispatch(capability, step_name, payload):
            if step_name == "flaky":
                raise RuntimeError("fail")
            return {}

        dag = DagDefinition(
            dag_id="d3",
            steps=[
                DagStep(
                    step_name="flaky",
                    capability="x",
                    on_failure=OnFailureAction.FALLBACK,
                    fallback=FallbackConfig(output_mapping={"result": "cached"}),
                ),
            ],
        )
        engine = DagEngine()
        ctx = await engine.execute(dag, dispatch)
        assert ctx.status.value == "completed"
        assert "flaky" in ctx.completed_steps

    @pytest.mark.asyncio
    async def test_dag_abort_on_failure(self):
        from src.models.dag import DagDefinition, DagStep, OnFailureAction
        from src.platform.dag_engine import DagEngine

        async def dispatch(capability, step_name, payload):
            raise RuntimeError("always fails")

        dag = DagDefinition(
            dag_id="d4",
            steps=[DagStep(step_name="bad", capability="x", on_failure=OnFailureAction.ABORT)],
        )
        engine = DagEngine()
        ctx = await engine.execute(dag, dispatch)
        assert ctx.status.value == "failed"

    @pytest.mark.asyncio
    async def test_topo_sort_parallel_wave(self):
        from src.models.dag import DagStep
        from src.platform.dag_engine import DagEngine

        engine = DagEngine()
        steps = [
            DagStep(step_name="a", capability="x"),
            DagStep(step_name="b", capability="x"),
            DagStep(step_name="c", capability="x", depends_on=["a", "b"]),
        ]
        waves = engine._topo_sort(steps)
        assert len(waves) == 2
        first_wave = set(waves[0])
        assert "a" in first_wave and "b" in first_wave
        assert waves[1] == ["c"]

    def test_topo_sort_linear(self):
        from src.models.dag import DagStep
        from src.platform.dag_engine import DagEngine

        engine = DagEngine()
        steps = [
            DagStep(step_name="a", capability="x"),
            DagStep(step_name="b", capability="x", depends_on=["a"]),
            DagStep(step_name="c", capability="x", depends_on=["b"]),
        ]
        waves = engine._topo_sort(steps)
        assert [w[0] for w in waves] == ["a", "b", "c"]
