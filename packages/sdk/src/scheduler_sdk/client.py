"""scheduler_sdk.client — SchedulerClient implementation.

This is a standalone implementation with no dependency on the framework's
internal modules. It only requires httpx and pydantic.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_POLL_INTERVAL = 2.0
_DEFAULT_TIMEOUT = 300.0


class SchedulerClient:
    """Async HTTP client for the async-scheduler REST API.

    Usage::

        async with SchedulerClient("http://scheduler:8000", api_key="secret") as client:
            task = await client.submit_and_wait("my_dag", {"x": 1})
            print(task["output_data"])
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        api_key: str = "",
        timeout: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout = timeout
        self._client: Any = None

    async def __aenter__(self) -> "SchedulerClient":
        import httpx
        headers = {}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers=headers,
            timeout=self._timeout,
        )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._client:
            await self._client.aclose()

    # ── Tasks ─────────────────────────────────────────────────────────────

    async def submit_task(
        self,
        dag_id: str,
        input_data: dict[str, Any] | None = None,
        *,
        priority: str = "normal",
        tenant_id: str = "",
        idempotency_key: str | None = None,
        callback_url: str | None = None,
        timeout_seconds: int = 3600,
    ) -> dict[str, Any]:
        """Submit a new task. Returns task creation response with task_id."""
        payload: dict[str, Any] = {
            "task_type": dag_id,
            "input_data": input_data or {},
            "priority": priority,
            "timeout_seconds": timeout_seconds,
        }
        if tenant_id:
            payload["tenant_id"] = tenant_id
        if idempotency_key:
            payload["idempotency_key"] = idempotency_key
        if callback_url:
            payload["callback_url"] = callback_url
        resp = await self._client.post("/tasks", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def get_task(self, task_id: str) -> dict[str, Any]:
        """Return full task information."""
        resp = await self._client.get(f"/tasks/{task_id}")
        resp.raise_for_status()
        return resp.json()

    async def cancel_task(self, task_id: str) -> dict[str, Any]:
        """Cancel a pending or running task."""
        resp = await self._client.delete(f"/tasks/{task_id}")
        resp.raise_for_status()
        return resp.json()

    async def list_tasks(
        self,
        tenant_id: str = "",
        status: str | None = None,
        limit: int = 20,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """List tasks with optional filters."""
        params: dict[str, Any] = {"limit": limit}
        if tenant_id:
            params["tenant_id"] = tenant_id
        if status:
            params["status"] = status
        if cursor:
            params["cursor"] = cursor
        resp = await self._client.get("/tasks", params=params)
        resp.raise_for_status()
        return resp.json()

    async def wait_for_task(
        self,
        task_id: str,
        poll_interval: float = _DEFAULT_POLL_INTERVAL,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> dict[str, Any]:
        """Poll until the task reaches a terminal state and return final task info.

        Raises:
            TimeoutError: if *timeout* seconds elapse.
            RuntimeError: if the task ends in failed or cancelled state.
        """
        terminal = {"completed", "failed", "cancelled"}
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            task = await self.get_task(task_id)
            status = task.get("status", "")
            if status in terminal:
                if status != "completed":
                    raise RuntimeError(
                        f"Task {task_id} ended with status={status!r}: "
                        f"{task.get('output_data', {})}"
                    )
                return task
            if loop.time() >= deadline:
                raise TimeoutError(
                    f"Task {task_id} did not complete within {timeout}s "
                    f"(current status: {status!r})"
                )
            await asyncio.sleep(poll_interval)

    async def submit_and_wait(
        self,
        dag_id: str,
        input_data: dict[str, Any] | None = None,
        *,
        priority: str = "normal",
        tenant_id: str = "",
        idempotency_key: str | None = None,
        callback_url: str | None = None,
        timeout_seconds: int = 3600,
        poll_interval: float = _DEFAULT_POLL_INTERVAL,
        wait_timeout: float = _DEFAULT_TIMEOUT,
    ) -> dict[str, Any]:
        """Submit a task and block until it completes. Returns final task info."""
        task = await self.submit_task(
            dag_id=dag_id,
            input_data=input_data,
            priority=priority,
            tenant_id=tenant_id,
            idempotency_key=idempotency_key,
            callback_url=callback_url,
            timeout_seconds=timeout_seconds,
        )
        return await self.wait_for_task(
            task["task_id"],
            poll_interval=poll_interval,
            timeout=wait_timeout,
        )

    # ── Health ────────────────────────────────────────────────────────────

    async def health(self) -> dict[str, Any]:
        """Return API health status."""
        resp = await self._client.get("/health/")
        resp.raise_for_status()
        return resp.json()

    async def is_ready(self) -> bool:
        """Return True if the API is ready to serve requests."""
        try:
            resp = await self._client.get("/health/ready", timeout=5.0)
            return resp.status_code == 200
        except Exception:
            return False
