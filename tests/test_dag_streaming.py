"""Tests for streaming DAG execution: ExecutionMode.STREAMING + SSE bus."""
from __future__ import annotations

import asyncio
import pytest
from typing import Any


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

async def make_streaming_handler(tokens: list):
    """Returns an async generator handler."""
    async def handler(payload):
        for tok in tokens:
            await asyncio.sleep(0.005)
            yield {"token": tok}
        yield {"done": True, "count": len(tokens)}
    return handler


# ──────────────────────────────────────────────────────────────────────────────
# ExecutionMode.STREAMING unit tests
# ──────────────────────────────────────────────────────────────────────────────

class TestStreamingExecutionMode:

    def test_streaming_mode_in_enum(self):
        from async_scheduler.dag.step_executors import ExecutionMode
        assert ExecutionMode.STREAMING == "streaming"
        assert ExecutionMode.STREAMING in list(ExecutionMode)

    @pytest.mark.asyncio
    async def test_execute_streaming_collects_chunks(self):
        """_execute_streaming collects all yielded chunks."""
        from async_scheduler.dag.step_executors import (
            StepExecutors, StepExecutionContext, ExecutionMode, StepStatus
        )

        async def gen_handler(payload):
            for i in range(5):
                await asyncio.sleep(0.001)
                yield {"n": i}
            yield {"done": True}

        se = StepExecutors()
        ctx = StepExecutionContext(
            task_type="gen", payload={}, timeout_seconds=10, step_id="s1"
        )
        result = await se.execute(ExecutionMode.STREAMING, ctx, gen_handler)

        assert result.status == StepStatus.COMPLETED
        assert result.value["chunk_count"] == 6
        assert len(result.value["chunks"]) == 6
        assert result.value["done"] is True

    @pytest.mark.asyncio
    async def test_execute_streaming_fires_on_chunk_callback(self):
        """on_chunk is called for every yielded chunk."""
        from async_scheduler.dag.step_executors import (
            StepExecutors, StepExecutionContext, ExecutionMode
        )

        received = []

        async def gen_handler(payload):
            for i in range(3):
                yield {"idx": i}

        se = StepExecutors()
        ctx = StepExecutionContext(
            task_type="gen", payload={}, timeout_seconds=10,
            step_id="test-node",
            on_chunk=lambda step_id, chunk: received.append((step_id, chunk)),
        )
        await se.execute(ExecutionMode.STREAMING, ctx, gen_handler)

        assert len(received) == 3
        assert all(sid == "test-node" for sid, _ in received)
        assert [c["idx"] for _, c in received] == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_execute_streaming_merges_dict_chunks(self):
        """Dict chunks are merged into result.value."""
        from async_scheduler.dag.step_executors import (
            StepExecutors, StepExecutionContext, ExecutionMode
        )

        async def gen_handler(payload):
            yield {"partial": "a"}
            yield {"partial": "b", "final": True}

        se = StepExecutors()
        ctx = StepExecutionContext(task_type="gen", payload={}, timeout_seconds=10)
        result = await se.execute(ExecutionMode.STREAMING, ctx, gen_handler)

        # Last write wins
        assert result.value["partial"] == "b"
        assert result.value["final"] is True

    @pytest.mark.asyncio
    async def test_execute_streaming_handles_plain_awaitable(self):
        """A plain async function (not generator) also works in STREAMING mode."""
        from async_scheduler.dag.step_executors import (
            StepExecutors, StepExecutionContext, ExecutionMode, StepStatus
        )

        async def regular_handler(payload):
            return {"result": 42}

        se = StepExecutors()
        ctx = StepExecutionContext(task_type="gen", payload={}, timeout_seconds=10)
        result = await se.execute(ExecutionMode.STREAMING, ctx, regular_handler)

        assert result.status == StepStatus.COMPLETED
        assert result.value["result"] == 42

    @pytest.mark.asyncio
    async def test_execute_streaming_handles_exception(self):
        """Generator that raises is captured as FAILED result."""
        from async_scheduler.dag.step_executors import (
            StepExecutors, StepExecutionContext, ExecutionMode, StepStatus
        )

        async def failing_gen(payload):
            yield {"partial": "ok"}
            raise RuntimeError("stream broken")

        se = StepExecutors()
        ctx = StepExecutionContext(task_type="gen", payload={}, timeout_seconds=10)
        result = await se.execute(ExecutionMode.STREAMING, ctx, failing_gen)

        assert result.status == StepStatus.FAILED
        assert "stream broken" in (result.error or "")


# ──────────────────────────────────────────────────────────────────────────────
# Streaming DAG integration: execution_mode in payload
# ──────────────────────────────────────────────────────────────────────────────

class TestStreamingDAGIntegration:

    @pytest.mark.asyncio
    async def test_llm_streaming_dag_succeeds(self):
        """Sample 1: LLM token streaming DAG runs end-to-end."""
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from examples.dag_streaming_samples import (
            make_llm_streaming_dag, run_streaming_dag
        )
        result = await run_streaming_dag(
            "LLM Streaming", make_llm_streaming_dag(), print_chunks=False
        )
        assert result["status"] == "success"
        assert result["node_statuses"]["generate"] == "success"
        assert result["node_statuses"]["postprocess"] == "success"

    @pytest.mark.asyncio
    async def test_llm_streaming_chunks_received(self):
        """LLM streaming emits chunks via on_chunk callback."""
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from examples.dag_streaming_samples import make_llm_streaming_dag, run_streaming_dag

        result = await run_streaming_dag(
            "LLM Chunks", make_llm_streaming_dag(), print_chunks=False
        )
        # generate node should have emitted 8 chunks (7 tokens + done)
        gen_chunks = [c for c in result["chunks"] if c["step"] == "generate"]
        assert len(gen_chunks) >= 7, f"Expected ≥7 chunks, got {len(gen_chunks)}"

    @pytest.mark.asyncio
    async def test_progress_streaming_dag_succeeds(self):
        """Sample 2: Progress streaming pipeline runs end-to-end."""
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from examples.dag_streaming_samples import (
            make_progress_streaming_dag, run_streaming_dag
        )
        result = await run_streaming_dag(
            "Progress", make_progress_streaming_dag(), print_chunks=False
        )
        assert result["status"] == "success"
        assert result["node_statuses"]["report"] == "success"

    @pytest.mark.asyncio
    async def test_realtime_etl_streaming_dag_succeeds(self):
        """Sample 3: Real-time ETL streaming pipeline runs end-to-end."""
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from examples.dag_streaming_samples import (
            make_realtime_etl_dag, run_streaming_dag
        )
        result = await run_streaming_dag(
            "ETL Streaming", make_realtime_etl_dag(), print_chunks=False
        )
        assert result["status"] == "success"
        assert result["node_statuses"]["sink"] == "success"

    @pytest.mark.asyncio
    async def test_scan_chunks_track_all_rows(self):
        """source_scan emits one chunk per row."""
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from examples.dag_streaming_samples import make_realtime_etl_dag, run_streaming_dag

        result = await run_streaming_dag(
            "ETL rows", make_realtime_etl_dag(), print_chunks=False
        )
        scan_chunks = [c for c in result["chunks"] if c["step"] == "scan"]
        # 8 rows + 1 scan_done
        assert len(scan_chunks) >= 8, f"Expected ≥8 scan chunks, got {len(scan_chunks)}"


# ──────────────────────────────────────────────────────────────────────────────
# SSE progress bus
# ──────────────────────────────────────────────────────────────────────────────

class TestSSEProgressBus:

    @pytest.mark.asyncio
    async def test_sse_bus_demo_receives_events(self):
        """SSE bus receives node_start, node_done and dag_done events."""
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from examples.dag_streaming_samples import demo_sse_progress_bus
        events = await demo_sse_progress_bus()
        assert len(events) > 0
        event_types = {e["event"] for e in events}
        assert "node_start" in event_types
        assert "node_done" in event_types
        assert "dag_done" in event_types

    @pytest.mark.asyncio
    async def test_api_sse_endpoints_registered(self):
        """SSE endpoints /dags/{id}/stream and /dags/stream exist in app."""
        import importlib
        mod = importlib.import_module("async_scheduler.api.app")
        routes = {r.path for r in mod.app.routes}
        assert "/dags/{dag_id}/stream" in routes
        assert "/dags/stream" in routes

    @pytest.mark.asyncio
    async def test_sse_generator_yields_heartbeat_on_idle(self):
        """_sse_generator yields heartbeat events while waiting."""
        from async_scheduler.api.app import _sse_generator, _get_or_create_bus

        dag_id = "test-sse-heartbeat"
        bus = _get_or_create_bus(dag_id)

        chunks = []
        gen = _sse_generator(dag_id, timeout=0.2)
        async for chunk in gen:
            chunks.append(chunk)
            if len(chunks) >= 2:
                break

        # Should have received heartbeat events
        assert any("heartbeat" in c for c in chunks)

    @pytest.mark.asyncio
    async def test_sse_generator_terminates_on_dag_done(self):
        """_sse_generator stops when dag_done event is pushed."""
        from async_scheduler.api.app import _sse_generator, _get_or_create_bus, _push_event
        import time as _t

        dag_id = "test-sse-done"
        bus = _get_or_create_bus(dag_id)

        async def push_done():
            await asyncio.sleep(0.05)
            _push_event(dag_id, {"event": "dag_done", "status": "success"})

        asyncio.create_task(push_done())

        chunks = []
        async for chunk in _sse_generator(dag_id, timeout=2.0):
            chunks.append(chunk)

        assert any("dag_done" in c for c in chunks)
        assert len(chunks) >= 1
