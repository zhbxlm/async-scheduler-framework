"""Step executors to move DAG execution closer to deepwiki layering.

The StepExecutors component provides a unified execution interface for DAG nodes
with support for multiple execution modes (sync, async, flask_wrapped), timeout
enforcement, execution tracking, and error handling.

This is Batch 2 of the deepwiki distributed-alignment roadmap: making
StepExecutors more real and central to DAG execution.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable
from datetime import datetime

import httpx

logger = logging.getLogger(__name__)

class ExecutionMode(str, Enum):
    """Execution modes for DAG steps.

    SYNC: Standard synchronous execution - handler runs and returns result directly.
    ASYNC: Asynchronous execution - returns immediately with invocation_id for later result lookup.
    FLASK_WRAPPED: Execution via external Flask service endpoint.
    STREAMING: Handler is an AsyncGenerator; chunks are collected and emitted via on_chunk callback.
    """
    SYNC = "sync"
    ASYNC = "async"
    FLASK_WRAPPED = "flask_wrapped"
    STREAMING = "streaming"

class StepStatus(str, Enum):
    """Status of a step execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"

@dataclass
class StepExecutionContext:
    """Context for executing a single DAG step."""
    task_type: str
    payload: dict[str, Any]
    timeout_seconds: int
    flask_url: str | None = None
    step_id: str | None = None
    dag_id: str | None = None
    on_chunk: "Callable[[str, Any], None] | None" = None  # streaming: called per chunk

@dataclass
class StepExecutionResult:
    """Result of a step execution."""
    status: StepStatus
    value: Any = None
    error: str | None = None
    invocation_id: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_ms: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "status": self.status.value,
            "value": self.value,
            "error": self.error,
            "invocation_id": self.invocation_id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_ms": self.duration_ms,
        }

@dataclass
class ExecutionMetrics:
    """Metrics for step executions."""
    total_executions: int = 0
    successful_executions: int = 0
    failed_executions: int = 0
    timeout_executions: int = 0
    cancelled_executions: int = 0
    total_duration_ms: float = 0.0

    def get_success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_executions == 0:
            return 0.0
        return (self.successful_executions / self.total_executions) * 100

    def get_avg_duration_ms(self) -> float:
        """Calculate average duration in milliseconds."""
        completed = self.successful_executions + self.failed_executions + self.timeout_executions
        if completed == 0:
            return 0.0
        return self.total_duration_ms / completed

class StepExecutors:
    """Executes a single step according to its execution mode.

    The StepExecutors component provides a unified interface for executing DAG nodes
    across different execution modes, with support for:
    - Synchronous and asynchronous execution
    - External service integration (Flask-wrapped)
    - Timeout enforcement
    - Execution tracking and metrics
    - Cancellation support

    This is the central execution point for DAG node execution, moving closer to
    the deepwiki layering where step execution is a distinct platform component.

    Args:
        enable_metrics: Whether to collect execution metrics (default: True).
    """

    def __init__(self, enable_metrics: bool = True) -> None:
        """Initialize the step executors."""
        self._async_results: dict[str, Any] = {}
        self._running_executions: dict[str, asyncio.Task[Any]] = {}
        self._cancellation_events: dict[str, asyncio.Event] = {}
        self._metrics = ExecutionMetrics() if enable_metrics else None
        self._enable_metrics = enable_metrics

    async def execute(
        self,
        mode: ExecutionMode,
        ctx: StepExecutionContext,
        handler: Callable[[str, dict[str, Any]], Awaitable[Any]],
    ) -> StepExecutionResult:
        """Execute a step according to its mode.

        Args:
            mode: The execution mode (sync, async, or flask_wrapped).
            ctx: The execution context containing task type, payload, timeout, etc.
            handler: The async handler function for the task type.

        Returns:
            A StepExecutionResult containing the execution outcome.

        Raises:
            ValueError: If the execution mode is unsupported.
        """
        if self._enable_metrics:
            self._metrics.total_executions += 1

        started_at = datetime.utcnow()
        result: StepExecutionResult

        try:
            if mode == ExecutionMode.SYNC:
                result = await self._execute_sync(ctx, handler)
            elif mode == ExecutionMode.ASYNC:
                result = await self._execute_async(ctx, handler)
            elif mode == ExecutionMode.FLASK_WRAPPED:
                result = await self._execute_flask_wrapped(ctx, handler)
            elif mode == ExecutionMode.STREAMING:
                result = await self._execute_streaming(ctx, handler)
            else:
                raise ValueError(f"Unsupported execution mode: {mode}")
        except asyncio.TimeoutError:
            result = StepExecutionResult(
                status=StepStatus.TIMEOUT,
                error=f"Step timed out after {ctx.timeout_seconds} seconds",
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )
            if self._enable_metrics:
                self._metrics.timeout_executions += 1
        except asyncio.CancelledError:
            result = StepExecutionResult(
                status=StepStatus.CANCELLED,
                error="Step execution was cancelled",
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )
            if self._enable_metrics:
                self._metrics.cancelled_executions += 1
        except Exception as e:
            result = StepExecutionResult(
                status=StepStatus.FAILED,
                error=str(e),
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )
            if self._enable_metrics:
                self._metrics.failed_executions += 1
            logger.exception(f"Step execution failed: {e}")

        result.started_at = started_at
        if result.completed_at:
            result.duration_ms = (result.completed_at - started_at).total_seconds() * 1000
            if self._enable_metrics and result.status == StepStatus.COMPLETED:
                self._metrics.total_duration_ms += result.duration_ms
                self._metrics.successful_executions += 1

        return result

    async def _execute_sync(
        self,
        ctx: StepExecutionContext,
        handler: Callable[[str, dict[str, Any]], Awaitable[Any]],
    ) -> StepExecutionResult:
        """Execute a step synchronously.

        The handler runs and the result is returned directly.
        """
        return StepExecutionResult(
            status=StepStatus.COMPLETED,
            value=await asyncio.wait_for(
                handler(ctx.task_type, ctx.payload),
                timeout=ctx.timeout_seconds,
            ),
            completed_at=datetime.utcnow(),
        )

    async def _execute_async(
        self,
        ctx: StepExecutionContext,
        handler: Callable[[str, dict[str, Any]], Awaitable[Any]],
    ) -> StepExecutionResult:
        """Execute a step asynchronously.

        Returns immediately with an invocation_id. The result can be retrieved
        later using get_async_result().
        """
        invocation_id = str(uuid.uuid4())
        cancel_event = asyncio.Event()
        self._cancellation_events[invocation_id] = cancel_event

        async def _runner() -> None:
            try:
                if cancel_event.is_set():
                    raise asyncio.CancelledError()
                self._async_results[invocation_id] = await asyncio.wait_for(
                    handler(ctx.task_type, ctx.payload),
                    timeout=ctx.timeout_seconds,
                )
            except asyncio.CancelledError:
                self._async_results[invocation_id] = {
                    "error": "cancelled",
                    "invocation_id": invocation_id,
                }
                raise
            except Exception as e:
                self._async_results[invocation_id] = {
                    "error": str(e),
                    "invocation_id": invocation_id,
                }

        task = asyncio.create_task(_runner())
        self._running_executions[invocation_id] = task

        try:
            await asyncio.wait_for(task, timeout=ctx.timeout_seconds)
            result = self._async_results.pop(invocation_id, None)
            return StepExecutionResult(
                status=StepStatus.COMPLETED,
                value={"invocation_id": invocation_id, "result": result},
                invocation_id=invocation_id,
                completed_at=datetime.utcnow(),
            )
        except asyncio.TimeoutError:
            raise
        finally:
            self._running_executions.pop(invocation_id, None)
            self._cancellation_events.pop(invocation_id, None)

    async def _execute_flask_wrapped(
        self,
        ctx: StepExecutionContext,
        handler: Callable[[str, dict[str, Any]], Awaitable[Any]],
    ) -> StepExecutionResult:
        """Execute a step via an external Flask service.

        If flask_url is provided, makes an HTTP POST request to the external service.
        Otherwise, falls back to local handler execution.
        """
        if ctx.flask_url:
            timeout = httpx.Timeout(ctx.timeout_seconds)
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(ctx.flask_url, json=ctx.payload)
                resp.raise_for_status()
                content_type = resp.headers.get("content-type", "")
                if "application/json" in content_type:
                    value = resp.json()
                else:
                    value = {"text": resp.text, "status_code": resp.status_code}
                return StepExecutionResult(
                    status=StepStatus.COMPLETED,
                    value=value,
                    completed_at=datetime.utcnow(),
                )
        # Fallback to local execution
        return await self._execute_sync(ctx, handler)

    async def get_async_result(self, invocation_id: str) -> Any | None:
        """Get the result of an async execution.

        Args:
            invocation_id: The invocation ID from the async execution.

        Returns:
            The result value, or None if not found or not yet complete.
        """
        return self._async_results.get(invocation_id)

    async def cancel_execution(self, invocation_id: str) -> bool:
        """Cancel an ongoing async execution.

        Args:
            invocation_id: The invocation ID to cancel.

        Returns:
            True if cancelled, False if not found.
        """
        if invocation_id in self._cancellation_events:
            self._cancellation_events[invocation_id].set()
            return True
        if invocation_id in self._running_executions:
            self._running_executions[invocation_id].cancel()
            return True
        return False

    async def _execute_streaming(
        self,
        ctx: StepExecutionContext,
        handler: Callable,
    ) -> StepExecutionResult:
        """Execute a streaming handler (AsyncGenerator).

        The handler must be an async generator that yields chunks.
        Chunks are:
          - Collected into a list stored in result.value["chunks"]
          - Passed one-by-one to ctx.on_chunk(step_id, chunk) if provided

        Final result.value is built by merging all dict chunks (last-write-wins)
        plus a ``chunks`` key holding the raw stream.

        Example handler::

            async def streaming_llm(payload):
                for token in ["Hello", " world", "!"]:
                    await asyncio.sleep(0.01)
                    yield {"token": token}
                yield {"done": True, "total_tokens": 3}
        """
        import inspect
        started = datetime.utcnow()
        chunks: list[Any] = []
        merged: dict[str, Any] = {}

        try:
            # Detect handler signature: (task_type, payload) vs (payload)
            _sig = inspect.signature(handler)
            _params = list(_sig.parameters)
            if len(_params) >= 2:
                result_or_gen = handler(ctx.task_type, ctx.payload)
            else:
                result_or_gen = handler(ctx.payload)

            # Support both regular async functions and async generators
            # Also handle the case where an awaitable returns an asyncgen
            if inspect.isasyncgen(result_or_gen):
                gen = result_or_gen
            elif inspect.isawaitable(result_or_gen):
                awaited = await result_or_gen
                if inspect.isasyncgen(awaited):
                    gen = awaited
                else:
                    # Plain return value, not a generator
                    chunks.append(awaited)
                    if isinstance(awaited, dict):
                        merged.update(awaited)
                    merged["chunks"] = chunks
                    merged["chunk_count"] = len(chunks)
                    completed = datetime.utcnow()
                    return StepExecutionResult(
                        status=StepStatus.COMPLETED,
                        value=merged,
                        started_at=started,
                        completed_at=completed,
                        duration_ms=(completed - started).total_seconds() * 1000,
                    )
                gen = awaited
            else:
                gen = result_or_gen

            async for chunk in gen:
                chunks.append(chunk)
                if isinstance(chunk, dict):
                    merged.update(chunk)
                if ctx.on_chunk is not None:
                    try:
                        ctx.on_chunk(ctx.step_id or ctx.task_type, chunk)
                    except Exception as _cb_err:
                        logger.debug("on_chunk callback error: %s", _cb_err)

            merged["chunks"] = chunks
            merged["chunk_count"] = len(chunks)

            return StepExecutionResult(
                status=StepStatus.COMPLETED,
                value=merged,
                started_at=started,
                completed_at=datetime.utcnow(),
            )

        except asyncio.CancelledError:
            return StepExecutionResult(
                status=StepStatus.CANCELLED,
                error="Streaming step cancelled",
                started_at=started,
                completed_at=datetime.utcnow(),
            )
        except Exception as exc:
            logger.error("Streaming step failed step_id=%s: %s", ctx.step_id, exc, exc_info=True)
            return StepExecutionResult(
                status=StepStatus.FAILED,
                error=str(exc),
                started_at=started,
                completed_at=datetime.utcnow(),
            )

    def get_metrics(self) -> ExecutionMetrics | None:
        """Get the current execution metrics.

        Returns:
            The ExecutionMetrics if metrics are enabled, None otherwise.
        """
        return self._metrics

    def reset_metrics(self) -> None:
        """Reset all execution metrics."""
        if self._metrics:
            self._metrics = ExecutionMetrics()

    def get_running_count(self) -> int:
        """Get the count of currently running async executions.

        Returns:
            Number of running executions.
        """
        return len(self._running_executions)
