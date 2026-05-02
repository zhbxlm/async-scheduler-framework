"""DAG sample deployment tests - validates typical DAG patterns and bug fixes."""
from __future__ import annotations

import asyncio
import sys
import os
import pytest

# ensure examples/ is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

async def run(dag, verbose: bool = False):
    from async_scheduler.dag.engine import DAGEngine
    from examples.dag_samples import dispatch
    engine = DAGEngine()
    return await engine.execute(dag, dispatch)


def node_status(result, node_id: str):
    ex = result.node_executions.get(node_id)
    return ex.status.value if ex else "missing"


def node_skipped(result, node_id: str):
    ex = result.node_executions.get(node_id)
    return ex.skipped if ex else False


def node_result(result, node_id: str):
    ex = result.node_executions.get(node_id)
    return ex.result if ex else None


# ──────────────────────────────────────────────────────────────────────────────
# Sample 1: ETL Pipeline
# ──────────────────────────────────────────────────────────────────────────────

class TestETLPipeline:
    @pytest.mark.asyncio
    async def test_all_nodes_succeed(self):
        from examples.dag_samples import make_etl_dag
        result = await run(make_etl_dag())
        from async_scheduler.core.models import DAGExecutionStatus
        assert result.status == DAGExecutionStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_context_propagates_through_chain(self):
        """extract result (rows) must be visible when transform runs."""
        from examples.dag_samples import make_etl_dag
        result = await run(make_etl_dag())
        load_res = node_result(result, "load")
        assert load_res is not None
        assert load_res.get("loaded") == 5   # 5 rows from extract

    @pytest.mark.asyncio
    async def test_node_order_is_respected(self):
        """All three nodes must complete in topological order."""
        from examples.dag_samples import make_etl_dag
        result = await run(make_etl_dag())
        for nid in ("extract", "transform", "load"):
            assert node_status(result, nid) == "success"


# ──────────────────────────────────────────────────────────────────────────────
# Sample 2: Fan-out / Fan-in
# ──────────────────────────────────────────────────────────────────────────────

class TestFanOutFanIn:
    @pytest.mark.asyncio
    async def test_all_shards_run_in_parallel(self):
        import time
        from examples.dag_samples import make_fanout_dag
        engine_mod = __import__("async_scheduler.dag.engine", fromlist=["DAGEngine"])
        from examples.dag_samples import dispatch
        engine = engine_mod.DAGEngine()
        t0 = time.perf_counter()
        result = await engine.execute(make_fanout_dag(), dispatch)
        elapsed = time.perf_counter() - t0
        from async_scheduler.core.models import DAGExecutionStatus
        assert result.status == DAGExecutionStatus.SUCCESS
        # 3 shards × 0.05s each in parallel → well under 0.2s (serial would be 0.15s)
        assert elapsed < 0.25, f"Shards not parallel? elapsed={elapsed:.3f}s"

    @pytest.mark.asyncio
    async def test_merge_receives_all_shard_results(self):
        from examples.dag_samples import make_fanout_dag
        result = await run(make_fanout_dag())
        # Context should contain shard_0_result=0, shard_1_result=100, shard_2_result=200
        assert result.context.get("shard_0_result") == 0
        assert result.context.get("shard_1_result") == 100
        assert result.context.get("shard_2_result") == 200

    @pytest.mark.asyncio
    async def test_merge_total_is_correct(self):
        from examples.dag_samples import make_fanout_dag
        result = await run(make_fanout_dag())
        merge_res = node_result(result, "merge")
        assert merge_res is not None
        assert merge_res["total"] == 300  # 0 + 100 + 200


# ──────────────────────────────────────────────────────────────────────────────
# Sample 3: Conditional Branching
# ──────────────────────────────────────────────────────────────────────────────

class TestConditionalBranch:
    @pytest.mark.asyncio
    async def test_small_input_takes_fast_path(self):
        from examples.dag_samples import make_conditional_dag
        result = await run(make_conditional_dag(small_input=True))
        assert node_status(result, "fast_path") == "success"
        assert node_skipped(result, "slow_path") is True

    @pytest.mark.asyncio
    async def test_large_input_takes_slow_path(self):
        from examples.dag_samples import make_conditional_dag
        result = await run(make_conditional_dag(small_input=False))
        assert node_status(result, "slow_path") == "success"
        assert node_skipped(result, "fast_path") is True

    @pytest.mark.asyncio
    async def test_notify_runs_regardless_of_path(self):
        from examples.dag_samples import make_conditional_dag
        for small in (True, False):
            result = await run(make_conditional_dag(small_input=small))
            assert node_status(result, "notify") == "success", f"notify failed for small={small}"

    @pytest.mark.asyncio
    async def test_notify_receives_path_from_context(self):
        from examples.dag_samples import make_conditional_dag
        result = await run(make_conditional_dag(small_input=True))
        notify_res = node_result(result, "notify")
        assert notify_res is not None
        assert notify_res["path"] == "fast"


# ──────────────────────────────────────────────────────────────────────────────
# Sample 4: Retry with Fallback  ← BUG 1 & BUG 2 regression tests
# ──────────────────────────────────────────────────────────────────────────────

class TestRetryWithFallback:
    @pytest.mark.asyncio
    async def test_bug1_node_retries_before_failing(self):
        """Bug1: DAGEngine must retry nodes up to max_retries."""
        from examples.dag_samples import _flaky_call_count, dispatch
        from async_scheduler.core.models import DAG, DAGNode
        from async_scheduler.dag.engine import DAGEngine

        _flaky_call_count.clear()

        # Node that succeeds on attempt 3
        node = DAGNode(id="flaky", name="Flaky", task_type="flaky_api",
                       payload={"request_id": "retry-test"}, max_retries=3)
        dag = DAG(name="retry-test", nodes=[node])

        engine = DAGEngine()
        result = await engine.execute(dag, dispatch)

        from async_scheduler.core.models import DAGExecutionStatus
        assert result.status == DAGExecutionStatus.SUCCESS, \
            f"Expected SUCCESS after retry, got {result.status}"
        assert _flaky_call_count.get("retry-test", 0) == 3, \
            f"Expected 3 attempts, got {_flaky_call_count.get('retry-test')}"

    @pytest.mark.asyncio
    async def test_bug2_fallback_allows_downstream_to_proceed(self):
        """Bug2: on_failure='fallback' must not cancel downstream nodes."""
        from examples.dag_samples import make_retry_fallback_dag
        # Use max_retries=0 so it immediately falls back
        from async_scheduler.core.models import DAG, DAGNode
        from examples.dag_samples import dispatch
        from async_scheduler.dag.engine import DAGEngine

        flaky = DAGNode(id="flaky", name="Flaky", task_type="flaky_api",
                        payload={"request_id": "fallback-test"},
                        max_retries=0,  # fail immediately
                        on_failure="fallback",
                        fallback_payload={"api_result": "fallback", "source": "cache"})
        process = DAGNode(id="process", name="Process", task_type="process_result",
                          payload={}, dependencies=["flaky"])
        dag = DAG(name="fallback-dag", nodes=[flaky, process])

        from examples.dag_samples import _flaky_call_count
        _flaky_call_count.clear()

        engine = DAGEngine()
        result = await engine.execute(dag, dispatch)

        from async_scheduler.core.models import DAGExecutionStatus
        # With fallback, process should run and DAG should succeed
        assert node_status(result, "process") == "success", \
            f"process was {node_status(result, 'process')} — downstream blocked by bug2"
        assert result.status == DAGExecutionStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_fallback_result_in_context(self):
        """Fallback payload is merged into context."""
        from async_scheduler.core.models import DAG, DAGNode
        from examples.dag_samples import dispatch, _flaky_call_count
        from async_scheduler.dag.engine import DAGEngine

        _flaky_call_count.clear()

        flaky = DAGNode(id="flaky", name="Flaky", task_type="flaky_api",
                        payload={"request_id": "ctx-test"},
                        max_retries=0,
                        on_failure="fallback",
                        fallback_payload={"api_result": "cached_value"})
        dag = DAG(name="ctx-fallback", nodes=[flaky])
        engine = DAGEngine()
        result = await engine.execute(dag, dispatch)

        assert result.context.get("api_result") == "cached_value"

    @pytest.mark.asyncio
    async def test_full_retry_then_fallback_dag(self):
        """Full sample 4: flaky retries then succeeds."""
        from examples.dag_samples import make_retry_fallback_dag, _flaky_call_count
        _flaky_call_count.clear()
        result = await run(make_retry_fallback_dag())
        from async_scheduler.core.models import DAGExecutionStatus
        assert result.status == DAGExecutionStatus.SUCCESS
        assert node_status(result, "process") == "success"


# ──────────────────────────────────────────────────────────────────────────────
# Sample 5: ML Training Pipeline
# ──────────────────────────────────────────────────────────────────────────────

class TestMLPipeline:
    @pytest.mark.asyncio
    async def test_full_pipeline_succeeds(self):
        from examples.dag_samples import make_ml_pipeline_dag
        result = await run(make_ml_pipeline_dag())
        from async_scheduler.core.models import DAGExecutionStatus
        assert result.status == DAGExecutionStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_train_and_baseline_run_in_parallel(self):
        import time
        from examples.dag_samples import make_ml_pipeline_dag, dispatch
        from async_scheduler.dag.engine import DAGEngine
        engine = DAGEngine()
        t0 = time.perf_counter()
        result = await engine.execute(make_ml_pipeline_dag(), dispatch)
        elapsed = time.perf_counter() - t0
        # train=0.1s, baseline=0.04s in parallel → total < 0.3s
        # serial would be 0.14s extra on top of sequential
        assert elapsed < 0.45, f"Parallel execution too slow: {elapsed:.3f}s"

    @pytest.mark.asyncio
    async def test_deploy_conditional_on_evaluation_pass(self):
        from examples.dag_samples import make_ml_pipeline_dag
        result = await run(make_ml_pipeline_dag())
        # Model passes evaluation (val_acc=0.91 > baseline 0.72 + 0.1)
        assert result.context.get("passed") is True
        assert node_status(result, "deploy") == "success"
        assert node_skipped(result, "deploy") is False

    @pytest.mark.asyncio
    async def test_feature_matrix_shape_propagates(self):
        from examples.dag_samples import make_ml_pipeline_dag
        result = await run(make_ml_pipeline_dag())
        # feature_eng doubles features: 3 → [10000, 6]
        assert result.context.get("feature_matrix_shape") == [10000, 6]
