"""
Streaming DAG samples — three real-world streaming patterns:

1. LLM Token Streaming      : async generator yields tokens one-by-one
2. Progress Report Pipeline : long-running nodes emit progress ticks
3. Real-time ETL with SSE   : combined node-level streaming + DAG-level SSE

Run with:
    python examples/dag_streaming_samples.py
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, AsyncGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("dag_streaming_samples")


# ──────────────────────────────────────────────────────────────────────────────
# Handler registry (streaming-aware)
# ──────────────────────────────────────────────────────────────────────────────

_HANDLERS: dict[str, Any] = {}
_STREAMING_HANDLERS: set[str] = set()  # handlers that return AsyncGenerators


def register(name: str, streaming: bool = False):
    def decorator(fn):
        _HANDLERS[name] = fn
        if streaming:
            _STREAMING_HANDLERS.add(name)
        return fn
    return decorator


async def dispatch(task_type: str, payload: dict[str, Any]) -> Any:
    fn = _HANDLERS.get(task_type)
    if fn is None:
        raise ValueError(f"No handler for task_type={task_type!r}")
    result = fn(payload)
    import inspect
    if inspect.isasyncgen(result):
        # Consume the generator and return merged result
        merged: dict = {}
        chunks = []
        async for chunk in result:
            chunks.append(chunk)
            if isinstance(chunk, dict):
                merged.update(chunk)
        merged["chunks"] = chunks
        return merged
    return await result


def make_streaming_dispatch():
    """Return a dispatch function that passes AsyncGenerators through for _execute_streaming."""
    import inspect
    async def _stream_dispatch(task_type: str, payload: dict[str, Any]):
        fn = _HANDLERS.get(task_type)
        if fn is None:
            raise ValueError(f"No handler for task_type={task_type!r}")
        result = fn(payload)
        if inspect.isasyncgen(result):
            return result   # return generator directly; _execute_streaming will consume it
        return await result
    return _stream_dispatch


# ──────────────────────────────────────────────────────────────────────────────
# Sample 1: LLM Token Streaming
# prompt → [tokenize, retrieve_context] → llm_generate (streaming) → postprocess
# ──────────────────────────────────────────────────────────────────────────────

@register("prepare_prompt")
async def handle_prepare_prompt(payload: dict) -> dict:
    question = payload.get("question", "")
    logger.info("[prepare_prompt] question=%r", question[:50])
    await asyncio.sleep(0.02)
    return {"prompt": f"Answer concisely: {question}", "tokens_in": len(question.split())}


@register("retrieve_context")
async def handle_retrieve_context(payload: dict) -> dict:
    logger.info("[retrieve_context] fetching relevant docs")
    await asyncio.sleep(0.03)
    return {"context_docs": ["doc-1: Python is a language", "doc-2: asyncio enables concurrency"]}


@register("llm_generate")
async def handle_llm_generate(payload: dict) -> AsyncGenerator[dict, None]:
    """Streaming: yields tokens one-by-one simulating LLM output."""
    prompt = payload.get("prompt", "")
    context = payload.get("context_docs", [])
    logger.info("[llm_generate] streaming response for prompt=%r", prompt[:40])

    # Simulate LLM token generation
    tokens = ["Python", " uses", " asyncio", " for", " concurrent", " I/O", "."]
    full_text = ""
    for i, token in enumerate(tokens):
        await asyncio.sleep(0.015)  # simulate token latency
        full_text += token
        yield {"token": token, "index": i, "partial_text": full_text}

    yield {"done": True, "full_text": full_text, "total_tokens": len(tokens)}


@register("postprocess")
async def handle_postprocess(payload: dict) -> dict:
    text = payload.get("full_text", "")
    logger.info("[postprocess] finalizing response len=%d", len(text))
    await asyncio.sleep(0.01)
    return {"final_answer": text.strip(), "word_count": len(text.split())}


def make_llm_streaming_dag() -> "DAG":
    from async_scheduler.core.models import DAG, DAGNode
    prepare = DAGNode(id="prepare", name="Prepare Prompt", task_type="prepare_prompt",
                      payload={"question": "How does Python asyncio work?"})
    retrieve = DAGNode(id="retrieve", name="Retrieve Context", task_type="retrieve_context",
                       payload={}, depends_on=["prepare"])
    generate = DAGNode(id="generate", name="LLM Generate", task_type="llm_generate",
                       payload={"execution_mode": "streaming"},
                       depends_on=["prepare", "retrieve"])
    postprocess = DAGNode(id="postprocess", name="Post-process", task_type="postprocess",
                          payload={}, depends_on=["generate"])
    return DAG(name="LLM Token Streaming", nodes=[prepare, retrieve, generate, postprocess])


# ──────────────────────────────────────────────────────────────────────────────
# Sample 2: Long-running Job with Progress Ticks
# init → [chunk_a, chunk_b, chunk_c] (streaming progress) → aggregate → report
# ──────────────────────────────────────────────────────────────────────────────

@register("job_init")
async def handle_job_init(payload: dict) -> dict:
    logger.info("[job_init] initializing batch job size=%s", payload.get("size"))
    await asyncio.sleep(0.02)
    return {"job_id": "job-2026", "total_items": payload.get("size", 100)}


@register("process_chunk")
async def handle_process_chunk(payload: dict) -> AsyncGenerator[dict, None]:
    """Streaming: emits progress every N items."""
    chunk_id = payload.get("chunk_id", 0)
    total = payload.get("total_items", 100) // 3
    logger.info("[chunk-%s] processing %d items (streaming progress)", chunk_id, total)

    processed = 0
    batch_size = max(1, total // 4)
    while processed < total:
        step = min(batch_size, total - processed)
        await asyncio.sleep(0.02)
        processed += step
        pct = round(processed / total * 100)
        yield {"chunk_id": chunk_id, "processed": processed, "total": total, "pct": pct}

    yield {"chunk_id": chunk_id, "done": True, "items_processed": processed}


@register("aggregate")
async def handle_aggregate(payload: dict) -> dict:
    logger.info("[aggregate] combining chunk results")
    await asyncio.sleep(0.01)
    # Count total items across chunks from context
    total = sum(
        payload.get(f"chunk_{i}_done_items_processed", 0) or
        (payload.get("items_processed") if payload.get("chunk_id") == i else 0)
        for i in range(3)
    )
    return {"aggregated": True, "total_processed": payload.get("total_items", 0)}


@register("final_report")
async def handle_final_report(payload: dict) -> dict:
    logger.info("[report] generating final report")
    await asyncio.sleep(0.01)
    return {"report_url": "/reports/job-2026", "status": "complete"}


def make_progress_streaming_dag() -> "DAG":
    from async_scheduler.core.models import DAG, DAGNode
    init = DAGNode(id="init", name="Init", task_type="job_init", payload={"size": 60})
    chunks = [
        DAGNode(id=f"chunk-{i}", name=f"Chunk {i}", task_type="process_chunk",
                payload={"chunk_id": i, "execution_mode": "streaming"},
                depends_on=["init"])
        for i in range(3)
    ]
    agg = DAGNode(id="agg", name="Aggregate", task_type="aggregate",
                  payload={}, depends_on=[f"chunk-{i}" for i in range(3)])
    report = DAGNode(id="report", name="Report", task_type="final_report",
                     payload={}, depends_on=["agg"])
    return DAG(name="Progress Streaming Pipeline",
               nodes=[init, *chunks, agg, report], max_parallelism=3)


# ──────────────────────────────────────────────────────────────────────────────
# Sample 3: Real-time ETL with per-row streaming
# source_scan (streaming) → validate_stream (streaming) → sink
# ──────────────────────────────────────────────────────────────────────────────

@register("source_scan")
async def handle_source_scan(payload: dict) -> AsyncGenerator[dict, None]:
    """Streaming: yields rows from a simulated data source."""
    rows = payload.get("rows", 8)
    logger.info("[source_scan] scanning %d rows (streaming)", rows)
    for i in range(rows):
        await asyncio.sleep(0.01)
        yield {"row_id": i, "value": i * 3, "source": "db"}
    yield {"scan_done": True, "total_rows": rows}


@register("validate_stream")
async def handle_validate_stream(payload: dict) -> AsyncGenerator[dict, None]:
    """Streaming: validates each row chunk from context."""
    chunks = payload.get("chunks", [])
    logger.info("[validate_stream] validating %d chunks", len(chunks))
    valid, invalid = 0, 0
    for chunk in chunks:
        await asyncio.sleep(0.005)
        if isinstance(chunk, dict) and "row_id" in chunk:
            ok = chunk["value"] % 2 == 0  # even values pass
            status = "valid" if ok else "invalid"
            if ok:
                valid += 1
            else:
                invalid += 1
            yield {"row_id": chunk["row_id"], "status": status}
    yield {"validation_done": True, "valid": valid, "invalid": invalid}


@register("sink_write")
async def handle_sink_write(payload: dict) -> dict:
    valid = payload.get("valid", 0)
    logger.info("[sink_write] writing %d valid rows", valid)
    await asyncio.sleep(0.02)
    return {"written": valid, "destination": "warehouse.clean_data"}


def make_realtime_etl_dag() -> "DAG":
    from async_scheduler.core.models import DAG, DAGNode
    scan = DAGNode(id="scan", name="Source Scan", task_type="source_scan",
                   payload={"rows": 8, "execution_mode": "streaming"})
    validate = DAGNode(id="validate", name="Validate Stream", task_type="validate_stream",
                       payload={"execution_mode": "streaming"}, depends_on=["scan"])
    sink = DAGNode(id="sink", name="Sink Write", task_type="sink_write",
                   payload={}, depends_on=["validate"])
    return DAG(name="Real-time ETL Streaming", nodes=[scan, validate, sink])


# ──────────────────────────────────────────────────────────────────────────────
# Streaming-aware engine runner (with on_chunk callback)
# ──────────────────────────────────────────────────────────────────────────────

async def run_streaming_dag(name: str, dag, print_chunks: bool = True):
    """Execute a streaming DAG using direct StepExecutors integration."""
    from async_scheduler.dag.engine import DAGEngine
    from async_scheduler.dag.step_executors import (
        StepExecutors, StepExecutionContext, ExecutionMode, StepExecutionResult, StepStatus
    )
    import inspect as _inspect

    received_chunks: list[dict] = []

    def on_chunk(step_id: str, chunk: Any):
        received_chunks.append({"step": step_id, "chunk": chunk})
        if print_chunks:
            logger.info("  [chunk] step=%-12s %s", step_id, chunk)

    # Override StepExecutors.execute to inject on_chunk for STREAMING nodes
    class ChunkAwareStepExecutors(StepExecutors):
        async def execute(self, mode, ctx, handler):
            node_mode = ctx.payload.get("execution_mode", "sync")
            if node_mode == "streaming" or mode == ExecutionMode.STREAMING:
                mode = ExecutionMode.STREAMING
                ctx = StepExecutionContext(
                    task_type=ctx.task_type,
                    payload=ctx.payload,
                    timeout_seconds=ctx.timeout_seconds,
                    flask_url=ctx.flask_url,
                    step_id=ctx.step_id,
                    dag_id=ctx.dag_id,
                    on_chunk=on_chunk,
                )
            return await super().execute(mode, ctx, handler)

    # Build engine with chunk-aware executors
    engine = DAGEngine(step_executors=ChunkAwareStepExecutors(enable_metrics=True))

    t0 = time.perf_counter()
    result = await engine.execute(dag, make_streaming_dispatch())
    elapsed = time.perf_counter() - t0

    icon = "✅" if result.status.value == "success" else "❌"
    logger.info("%s %s [%s] %.3fs  chunks_received=%d",
                icon, name, result.status.value, elapsed, len(received_chunks))

    return {
        "name": name,
        "status": result.status.value,
        "elapsed_s": round(elapsed, 3),
        "chunks": received_chunks,
        "node_statuses": {nid: ex.status.value for nid, ex in result.node_executions.items()},
    }


# ──────────────────────────────────────────────────────────────────────────────
# SSE progress bus demo (in-process)
# ──────────────────────────────────────────────────────────────────────────────

async def demo_sse_progress_bus():
    """Demonstrate the in-process SSE event bus without HTTP."""
    logger.info("\n--- SSE Progress Bus Demo ---")
    from asyncio import Queue

    events: list[dict] = []
    bus: Queue = Queue()

    async def producer():
        """Simulate DAG execution pushing events to the bus."""
        dag = make_llm_streaming_dag()
        for node in dag.nodes:
            await asyncio.sleep(0.03)
            bus.put_nowait({"event": "node_start", "node": node.id})
            await asyncio.sleep(0.03)
            bus.put_nowait({"event": "node_done", "node": node.id, "status": "success"})
        bus.put_nowait({"event": "dag_done", "dag_id": dag.id, "status": "success"})

    async def consumer():
        while True:
            try:
                ev = await asyncio.wait_for(bus.get(), timeout=2.0)
                events.append(ev)
                logger.info("  [SSE] %s", ev)
                if ev.get("event") in ("dag_done", "dag_failed"):
                    break
            except asyncio.TimeoutError:
                break

    await asyncio.gather(producer(), consumer())
    logger.info("SSE demo: %d events received", len(events))
    return events


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

async def main():
    print("\n" + "=" * 65)
    print("  Streaming DAG Deployment Tests")
    print("=" * 65 + "\n")

    results = []

    # 1. LLM Token Streaming
    print("--- Sample 1: LLM Token Streaming ---")
    results.append(await run_streaming_dag("LLM Token Streaming", make_llm_streaming_dag()))

    # 2. Progress Streaming
    print("\n--- Sample 2: Progress Streaming Pipeline ---")
    results.append(await run_streaming_dag("Progress Streaming", make_progress_streaming_dag()))

    # 3. Real-time ETL
    print("\n--- Sample 3: Real-time ETL Streaming ---")
    results.append(await run_streaming_dag("Real-time ETL", make_realtime_etl_dag()))

    # 4. SSE bus demo
    print("\n--- Sample 4: SSE Progress Bus Demo ---")
    sse_events = await demo_sse_progress_bus()

    print("\n" + "=" * 65)
    print("  Summary")
    print("=" * 65)
    for r in results:
        icon = "✅" if r["status"] == "success" else "❌"
        print(f"  {icon} {r['name']:40s} {r['status']:10s} {r['elapsed_s']:.3f}s"
              f"  chunks={len(r['chunks'])}")
    print(f"  📡 SSE events received: {len(sse_events)}")
    print()

    failures = [r for r in results if r["status"] != "success"]
    if failures:
        print(f"  ⚠️  {len(failures)} streaming DAG(s) failed:")
        for f in failures:
            print(f"    - {f['name']}: {f['status']}")
            for nid, st in f["node_statuses"].items():
                if st != "success":
                    print(f"      node {nid}: {st}")
    else:
        print("  All streaming DAGs completed successfully!")
    print()
    return results


if __name__ == "__main__":
    asyncio.run(main())
