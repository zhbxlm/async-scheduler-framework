"""StepExecutors — aligned with docs/deepwiki-reference/DAG 编排.md

Wraps the three DagEngine execution modes into a unified executor interface:
  - SyncStepExecutor  → Ray Actor synchronous call
  - AsyncStepExecutor → submit_async + Redis Pub/Sub notification + poll fallback
  - MapStepExecutor   → scatter-gather over shards with asyncio.gather

These are *adapter* classes used by DagEngine to dispatch step execution.
DagEngine calls: await executor.execute(step, context, dispatch_fn)
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)

DispatchFn = Callable[[str, str, dict[str, Any]], Awaitable[dict[str, Any]]]


class SyncStepExecutor:
    """Dispatch step synchronously via the provided dispatch callable.

    In practice, the dispatch callable resolves to a Ray Actor remote call
    (SchedulerActor.submit_to_pool) or a Flask HTTP call. The executor
    wraps retry + circuit-breaker recording.
    """

    def __init__(
        self,
        queue_manager: Any | None = None,
        *,
        max_retries: int = 0,
        retry_delay_seconds: float = 1.0,
    ) -> None:
        self._qm = queue_manager
        self._max_retries = max_retries
        self._retry_delay = retry_delay_seconds

    async def execute(
        self,
        capability: str,
        step_name: str,
        input_data: dict[str, Any],
        dispatch: DispatchFn,
    ) -> dict[str, Any]:
        """Execute one SYNC step via dispatch; retry on transient failure."""
        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                result = await dispatch(capability, step_name, input_data)
                if self._qm:
                    await self._qm.try_recover_concurrent(capability)
                return result
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "SyncStepExecutor: attempt=%d step=%s failed: %s",
                    attempt + 1, step_name, exc,
                )
                if self._qm:
                    await self._qm.adjust_concurrent(capability, -1)
                if attempt < self._max_retries:
                    await asyncio.sleep(self._retry_delay * (2 ** attempt))

        raise RuntimeError(
            f"SyncStepExecutor: step={step_name} failed after {self._max_retries + 1} attempts"
        ) from last_exc


class AsyncStepExecutor:
    """Submit step async then wait for completion via Redis Pub/Sub + polling fallback.

    Flow:
      1. dispatch(capability, step_name, input_data)  → submit
      2. Subscribe to Redis pubsub channel result:{task_id}:{step_name}
      3. Wait for message (with poll_interval fallback)
      4. Return result JSON from Redis GET result key
    """

    def __init__(
        self,
        redis_client: Any | None = None,
        *,
        poll_interval: float = 5.0,
        timeout_seconds: float = 300.0,
    ) -> None:
        self._r = redis_client
        self._poll_interval = poll_interval
        self._timeout = timeout_seconds

    async def execute(
        self,
        capability: str,
        step_name: str,
        input_data: dict[str, Any],
        dispatch: DispatchFn,
        task_id: str = "",
    ) -> dict[str, Any]:
        """Submit and await async step result."""
        # Submit
        submit_resp = await dispatch(capability, step_name, input_data)

        if self._r is None:
            # No Redis: fall back to sync result from dispatch
            return submit_resp

        # Wait for completion notification
        result_key = f"result:{task_id}:{step_name}"
        pubsub_channel = f"result_channel:{task_id}:{step_name}"

        deadline = asyncio.get_event_loop().time() + self._timeout
        psub = self._r.pubsub()
        await psub.subscribe(pubsub_channel)

        try:
            while asyncio.get_event_loop().time() < deadline:
                # Check result key first (may already be set)
                raw = await self._r.get(result_key)
                if raw:
                    import json
                    return json.loads(raw) if isinstance(raw, (str, bytes)) else raw

                # Wait for pubsub message
                try:
                    msg = await asyncio.wait_for(
                        psub.get_message(ignore_subscribe_messages=True, timeout=self._poll_interval),
                        timeout=self._poll_interval + 1,
                    )
                    if msg and msg.get("type") == "message":
                        raw = await self._r.get(result_key)
                        if raw:
                            import json
                            return json.loads(raw) if isinstance(raw, (str, bytes)) else raw
                except asyncio.TimeoutError:
                    pass

            raise asyncio.TimeoutError(
                f"AsyncStepExecutor: step={step_name} timed out after {self._timeout}s"
            )
        finally:
            await psub.unsubscribe(pubsub_channel)
            await psub.close()


class MapStepExecutor:
    """Scatter-gather executor for MAP steps.

    Reads shard list from context[map_over], dispatches each shard
    as an independent step call in parallel (up to max_parallelism),
    and gathers results into a list.
    """

    def __init__(
        self,
        *,
        max_parallelism: int = 16,
    ) -> None:
        self._max_parallelism = max_parallelism
        self._step_semaphore: asyncio.Semaphore | None = None

    async def execute(
        self,
        capability: str,
        step_name: str,
        input_data: dict[str, Any],
        dispatch: DispatchFn,
        shards: list[Any] | None = None,
        step: Any = None,  # DagStep with max_concurrency field
    ) -> list[dict[str, Any]]:
        """Execute step once per shard in parallel; return list of results."""
        if not shards:
            logger.warning("MapStepExecutor: no shards for step=%s", step_name)
            return []

        # Use step.max_concurrency if available (default to 16)
        max_concurrency = getattr(step, "max_concurrency", 0) or self._max_parallelism
        semaphore = asyncio.Semaphore(max_concurrency)

        async def _one(shard: Any, idx: int) -> dict[str, Any]:
            async with semaphore:
                shard_input = {**input_data, "shard": shard, "shard_index": idx}
                return await dispatch(capability, f"{step_name}[{idx}]", shard_input)

        tasks = [asyncio.create_task(_one(s, i)) for i, s in enumerate(shards)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        out = []
        errors = []
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                errors.append(f"shard[{i}]: {r}")
            else:
                out.append(r)

        if errors:
            raise RuntimeError(
                f"MapStepExecutor: step={step_name} {len(errors)} shards failed: "
                + "; ".join(errors)
            )
        return out
