"""Python SDK for the async-scheduler-framework REST API.

Provides a high-level async client that wraps all API endpoints.
Suitable for use in worker services, integration tests, and scripts.

Usage:
    from scheduler_sdk.client import SchedulerClient

    async with SchedulerClient("http://localhost:8000", api_key="...") as client:
        # Submit a task
        task = await client.submit_task(
            dag_id="train_v1",
            capability="gpu_training",
            input_data={"batch_size": 32},
        )
        print(task["task_id"])

        # Poll until done
        result = await client.wait_for_task(task["task_id"])
        print(result["output_data"])
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_POLL_INTERVAL = 2.0
_DEFAULT_TIMEOUT = 300.0


class SchedulerClient:
    """Async HTTP client for the scheduler REST API.

    Manages an httpx.AsyncClient session with automatic auth headers.
    Use as an async context manager for proper connection cleanup.
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

    # ── Task operations ───────────────────────────────────────────────────

    async def submit_task(
        self,
        dag_id: str,
        capability: str,
        input_data: dict[str, Any] | None = None,
        priority: str = "normal",
        tenant_id: str = "",
        idempotency_key: str | None = None,
        callback_url: str | None = None,
        timeout_seconds: int = 3600,
    ) -> dict[str, Any]:
        """Submit a new task and return the creation response.

        Args:
            dag_id: DAG identifier (mapped to task_type in API).
            capability: Internal routing hint (currently not sent to API;
                       routing uses task_type/dag_id internally).
            input_data: Task input parameters.
            priority: Task priority ("very_high", "high", "normal", "low", "tide").
            tenant_id: Tenant identifier for multi-tenancy.
            idempotency_key: Unique key for idempotent task creation.
            callback_url: URL to send callback when task completes.
            timeout_seconds: Maximum execution time in seconds.

        Returns:
            Task creation response with task_id and status.
        """
        payload: dict[str, Any] = {
            "task_type": dag_id,
            "input_data": input_data or {},
            "priority": priority,
            "tenant_id": tenant_id,
            "timeout_seconds": timeout_seconds,
        }
        if idempotency_key:
            payload["idempotency_key"] = idempotency_key
        if callback_url:
            payload["callback_url"] = callback_url

        resp = await self._client.post("/tasks", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def get_task(self, task_id: str) -> dict[str, Any]:
        """Get full task information by ID."""
        resp = await self._client.get(f"/tasks/{task_id}")
        resp.raise_for_status()
        return resp.json()

    async def get_task_result(self, task_id: str) -> dict[str, Any]:
        """Get task result (status + output_data)."""
        resp = await self._client.get(f"/tasks/{task_id}/result")
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
        """Poll until task reaches a terminal state.

        Returns the final task info dict.
        Raises TimeoutError if *timeout* seconds elapse.
        Raises RuntimeError if the task fails or is cancelled.
        """
        terminal = {"completed", "failed", "cancelled"}
        deadline = asyncio.get_running_loop().time() + timeout

        while True:
            task = await self.get_task(task_id)
            status = task.get("status", "")

            if status in terminal:
                if status != "completed":
                    raise RuntimeError(
                        f"Task {task_id} ended with status={status}: "
                        f"{task.get('output_data', {})}"
                    )
                return task

            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(
                    f"Task {task_id} did not complete within {timeout}s "
                    f"(current status: {status})"
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
        """Submit a task and wait for completion. Returns final task info.

        Convenience wrapper around submit_task + wait_for_task.
        Raises RuntimeError if task fails or is cancelled.
        Raises TimeoutError if wait_timeout elapses.
        """
        task = await self.submit_task(
            dag_id=dag_id,
            capability="",  # Not used by API, kept for signature compatibility
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

    # ── Capability operations ─────────────────────────────────────────────

    async def list_capabilities(self, tenant_id: str = "") -> list[dict]:
        """List all registered capabilities."""
        params = {"tenant_id": tenant_id} if tenant_id else {}
        resp = await self._client.get("/ops/v1/capabilities/", params=params)
        resp.raise_for_status()
        return resp.json()

    # ── Cluster operations ────────────────────────────────────────────────

    async def list_clusters(self, tenant_id: str = "") -> list[dict]:
        """List all registered clusters."""
        params = {"tenant_id": tenant_id} if tenant_id else {}
        resp = await self._client.get("/ops/v1/clusters/", params=params)
        resp.raise_for_status()
        return resp.json()

    # ── Health ────────────────────────────────────────────────────────────

    async def health(self) -> dict[str, Any]:
        """Check API health."""
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
