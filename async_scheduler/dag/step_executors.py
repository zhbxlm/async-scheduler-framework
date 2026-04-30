"""Step executors to move DAG execution closer to deepwiki layering."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable

import httpx


class ExecutionMode(str, Enum):
    SYNC = "sync"
    ASYNC = "async"
    FLASK_WRAPPED = "flask_wrapped"


@dataclass
class StepExecutionContext:
    task_type: str
    payload: dict[str, Any]
    timeout_seconds: int
    flask_url: str | None = None


class StepExecutors:
    """Executes a single step according to its execution mode.

    Current implementation is intentionally lightweight, but it establishes the
    structural seam that deepwiki relies on.
    """

    def __init__(self) -> None:
        self._async_results: dict[str, Any] = {}

    async def execute(
        self,
        mode: ExecutionMode,
        ctx: StepExecutionContext,
        handler: Callable[[str, dict[str, Any]], Awaitable[Any]],
    ) -> Any:
        if mode == ExecutionMode.SYNC:
            return await self._execute_sync(ctx, handler)
        if mode == ExecutionMode.ASYNC:
            return await self._execute_async(ctx, handler)
        if mode == ExecutionMode.FLASK_WRAPPED:
            return await self._execute_flask_wrapped(ctx, handler)
        raise ValueError(f"Unsupported execution mode: {mode}")

    async def _execute_sync(
        self,
        ctx: StepExecutionContext,
        handler: Callable[[str, dict[str, Any]], Awaitable[Any]],
    ) -> Any:
        return await asyncio.wait_for(handler(ctx.task_type, ctx.payload), timeout=ctx.timeout_seconds)

    async def _execute_async(
        self,
        ctx: StepExecutionContext,
        handler: Callable[[str, dict[str, Any]], Awaitable[Any]],
    ) -> Any:
        invocation_id = str(uuid.uuid4())

        async def _runner() -> None:
            self._async_results[invocation_id] = await handler(ctx.task_type, ctx.payload)

        task = asyncio.create_task(_runner())
        await asyncio.wait_for(task, timeout=ctx.timeout_seconds)
        return {
            "invocation_id": invocation_id,
            "result": self._async_results.pop(invocation_id, None),
        }

    async def _execute_flask_wrapped(
        self,
        ctx: StepExecutionContext,
        handler: Callable[[str, dict[str, Any]], Awaitable[Any]],
    ) -> Any:
        if ctx.flask_url:
            timeout = httpx.Timeout(ctx.timeout_seconds)
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(ctx.flask_url, json=ctx.payload)
                resp.raise_for_status()
                content_type = resp.headers.get("content-type", "")
                if "application/json" in content_type:
                    return resp.json()
                return {"text": resp.text, "status_code": resp.status_code}
        return await asyncio.wait_for(handler(ctx.task_type, ctx.payload), timeout=ctx.timeout_seconds)
