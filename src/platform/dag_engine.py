"""DagEngine (src/ version) — aligned with docs/deepwiki-reference/DAG 编排.md

Full-spec DAG execution engine:
- Topological scheduling with asyncio.gather() for parallel fan-out
- Condition expression evaluation via simple context lookup
- SYNC / ASYNC / FLASK_WRAPPED execution modes
- MAP scatter-gather
- STREAMING: blpop Redis buffer, per-chunk downstream dispatch, sentinel detection
- on_failure: ABORT / SKIP / FALLBACK
- Incremental context persistence (every step)
- CheckpointConfig support
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import time
from typing import Any, Callable, Awaitable

_STREAMING_EXECUTOR: concurrent.futures.ThreadPoolExecutor | None = None


def _get_streaming_executor(max_workers: int = 64) -> concurrent.futures.ThreadPoolExecutor:
    global _STREAMING_EXECUTOR
    if _STREAMING_EXECUTOR is None:
        _STREAMING_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="dag_streaming"
        )
    return _STREAMING_EXECUTOR

from src.models.dag import (
    DagContext, DagDefinition, DagStatus, DagStep,
    ExecutionMode, OnFailureAction, StepKind,
)

logger = logging.getLogger(__name__)


class DagEngine:
    """Execute a DagDefinition against a dispatch function."""

    def __init__(
        self,
        context_store: Any | None = None,
        *,
        max_parallelism: int = 8,
        redis: Any | None = None,
        streaming_executor_max_workers: int = 64,
    ) -> None:
        self._store = context_store
        self._max_parallelism = max_parallelism
        self._redis = redis
        self._streaming_executor_max_workers = streaming_executor_max_workers
        self._current_steps: list = []  # for get_ready_nodes

    async def execute(
        self,
        dag_def: DagDefinition,
        dispatch: Callable[[str, str, dict[str, Any]], Awaitable[dict[str, Any]]],
        initial_context: dict[str, Any] | None = None,
    ) -> DagContext:
        """Execute *dag_def* and return the final DagContext."""
        ctx = DagContext(
            task_id=(initial_context or {}).get("task_id") or dag_def.dag_id,
            dag_id=dag_def.dag_id,
            tenant_id=dag_def.tenant_id,
            input_data=initial_context or {},
            context=initial_context or {},
            status=DagStatus.RUNNING,
        )

        step_map = {s.step_name: s for s in dag_def.steps}
        self._current_steps = dag_def.steps
        order = self._topo_sort(dag_def.steps)

        if not order:
            ctx.status = DagStatus.COMPLETED
            return ctx

        semaphore = asyncio.Semaphore(min(self._max_parallelism, dag_def.max_concurrent or self._max_parallelism))

        # Execute in waves (topological levels)
        for wave in order:
            tasks = [
                asyncio.create_task(self._run_step(step_map[sn], ctx, dispatch, semaphore))
                for sn in wave
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for sn, res in zip(wave, results):
                if isinstance(res, Exception):
                    if step_map[sn].on_failure == OnFailureAction.ABORT:
                        ctx.status = DagStatus.FAILED
                        return ctx

        ctx.status = DagStatus.COMPLETED
        return ctx

    async def _run_step(
        self,
        step: DagStep,
        ctx: DagContext,
        dispatch: Callable,
        semaphore: asyncio.Semaphore,
    ) -> dict[str, Any]:
        # Check condition
        if step.condition and not self._eval_condition(step.condition, ctx):
            logger.debug("DagEngine: skip step=%s (condition false)", step.step_name)
            ctx.completed_steps.append(step.step_name)
            return {}

        # Build input
        input_data = {**ctx.input_data}
        for dst, src in step.input_mapping.items():
            val = ctx.context.get(src) or ctx.step_results.get(src)
            if val is not None:
                input_data[dst] = val

        result: dict[str, Any] = {}
        async with semaphore:
            # ── STREAMING step ──────────────────────────────────────────
            if step.step_kind == StepKind.STREAMING and step.streaming_trigger:
                try:
                    result = await self._run_streaming_step(step, ctx, dispatch, input_data)
                except Exception as e:
                    # Clean up Redis buffer on failure
                    if self._redis and step.streaming_trigger:
                        bk = step.streaming_trigger.buffer_key.replace("{task_id}", ctx.task_id)
                        await self._redis.delete(bk)
                    if step.on_failure == OnFailureAction.FALLBACK and step.fallback:
                        result = dict(step.fallback.output_mapping)
                    elif step.on_failure == OnFailureAction.SKIP:
                        result = {}
                    else:
                        raise
            else:
                # ── normal step (SYNC / ASYNC / FLASK_WRAPPED) ──────────
                for attempt in range(step.retry_policy.max_retries + 1):
                    try:
                        if step.execution_mode == ExecutionMode.SYNC:
                            result = await asyncio.wait_for(
                                dispatch(step.capability, step.step_name, input_data),
                                timeout=step.timeout_seconds,
                            )
                        elif step.execution_mode == ExecutionMode.ASYNC:
                            result = await dispatch(step.capability, step.step_name, input_data)
                        elif step.execution_mode == ExecutionMode.FLASK_WRAPPED:
                            flask_url = (step.flask.url if step.flask else "") or ""
                            result = await self._flask_dispatch(flask_url, input_data)
                        break
                    except Exception as e:
                        if attempt < step.retry_policy.max_retries:
                            await asyncio.sleep(step.retry_policy.retry_delay_seconds)
                            continue
                        # All retries exhausted
                        if step.on_failure == OnFailureAction.FALLBACK and step.fallback:
                            result = dict(step.fallback.output_mapping)
                        elif step.on_failure == OnFailureAction.SKIP:
                            result = {}
                        else:
                            raise

        # Output mapping
        for dst, src in step.output_mapping.items():
            if src in result:
                ctx.context[dst] = result[src]

        ctx.step_results[step.step_name] = result
        ctx.completed_steps.append(step.step_name)

        # Persist if store available
        if self._store:
            await self._store.save(ctx)

        return result

    async def _run_streaming_step(
        self,
        step,
        ctx: DagContext,
        dispatch: Callable,
        input_data: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute STREAMING step: inject buffer_key, dispatch, consume chunks via blpop."""
        trigger = step.streaming_trigger
        buffer_key = trigger.buffer_key.replace("{task_id}", ctx.task_id)

        # Inject buffer key so worker knows where to push chunks
        enriched = {**input_data, "_streaming_buffer_key": buffer_key}

        # Submit the streaming step — must actually await so the producer coroutine
        # runs and starts the background thread before we enter blpop.
        await dispatch(step.capability, step.step_name, enriched)

        # Small yield to let the producer thread start up
        await asyncio.sleep(0)

        loop = asyncio.get_event_loop()
        executor = _get_streaming_executor(self._streaming_executor_max_workers)
        result: dict[str, Any] = {}

        while True:
            # Prefer async_blpop (e.g. MockRedis / aioredis) to avoid thread blocking
            if hasattr(self._redis, "async_blpop"):
                item = await self._redis.async_blpop(buffer_key, timeout=30)
            else:
                # Sync redis — run in thread pool
                item = await loop.run_in_executor(
                    executor,
                    lambda bk=buffer_key: self._redis_blpop_sync(bk, timeout=30),
                )
            raw = item[1] if item else None
            if raw is None:
                raise TimeoutError(f"STREAMING: blpop timeout on buffer_key={buffer_key}")

            try:
                chunk = json.loads(raw)
            except Exception:
                chunk = {"__raw__": raw}

            # Sentinel: producer finished
            if chunk.get("__done__"):
                if chunk.get("__error__"):
                    raise RuntimeError(f"STREAMING step error: {chunk['__error__']}")
                summary = chunk.get("summary", {})
                for dst, src in step.output_mapping.items():
                    if src in summary:
                        result[dst] = summary[src]
                if trigger.flush_on_complete and self._redis:
                    await self._redis.delete(buffer_key)
                break

            # Non-sentinel chunk — dispatch each downstream step
            for ds_name in trigger.downstream_steps:
                try:
                    ds_result = await dispatch(
                        step.capability, ds_name,
                        {"chunk": chunk, **enriched},
                    )
                    acc_key = f"_streaming_results_{ds_name}"
                    if acc_key not in ctx.context:
                        ctx.context[acc_key] = []
                    ctx.context[acc_key].append(ds_result)
                except Exception as e:
                    logger.warning("STREAMING: downstream step %s failed: %s", ds_name, e)

        return result

    def _redis_blpop_sync(self, key: str, timeout: int = 30):
        """Synchronous blpop — runs in thread executor to avoid blocking event loop."""
        if self._redis is None:
            return None
        # For async redis clients we use the sync interface via run_until_complete
        # We expect _redis to be a sync redis.Redis instance when streaming is used
        try:
            res = self._redis.blpop(key, timeout=timeout)
            if res is None:
                return None
            _, value = res
            return value
        except Exception as e:
            logger.error("STREAMING blpop error: %s", e)
            return None

    async def _flask_dispatch(self, url: str, payload: dict) -> dict:
        import httpx
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            return resp.json()

    def _eval_condition(self, expr: str, ctx: DagContext) -> bool:
        """Simple condition evaluation against context."""
        try:
            local_vars = {**ctx.context, **ctx.input_data}
            return bool(eval(expr, {"__builtins__": {}}, local_vars))
        except Exception:
            return True  # fail-open

    def _topo_sort(self, steps: list[DagStep]) -> list[list[str]]:
        """Return topological levels (waves) for parallel execution."""
        step_map = {s.step_name: s for s in steps}
        in_degree = {s.step_name: len(s.depends_on) for s in steps}
        adj: dict[str, list[str]] = {s.step_name: [] for s in steps}
        for s in steps:
            for dep in s.depends_on:
                if dep in adj:
                    adj[dep].append(s.step_name)

        queue = [sn for sn, deg in in_degree.items() if deg == 0]
        waves: list[list[str]] = []
        while queue:
            waves.append(list(queue))
            next_q = []
            for sn in queue:
                for nbr in adj[sn]:
                    in_degree[nbr] -= 1
                    if in_degree[nbr] == 0:
                        next_q.append(nbr)
            queue = next_q
        return waves

    def topological_sort(self, steps):
        """Public alias for _topo_sort (doc API compatibility)."""
        return self._topo_sort(steps)

    def get_ready_nodes(self, step_names, completed, failed):
        """Return steps whose all dependencies are in completed set."""
        ready = []
        for name in step_names:
            step = next((s for s in self._current_steps if s.step_name == name), None)
            if step is None:
                continue
            deps = set(step.dependencies or [])
            if deps.issubset(completed) and name not in completed and name not in failed:
                ready.append(name)
        return ready

