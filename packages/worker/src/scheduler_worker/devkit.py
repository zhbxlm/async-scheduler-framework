"""scheduler_worker.devkit — local development and testing kit.

Runs your worker in-process without any scheduler infrastructure.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


class WorkerDevKit:
    """Run a worker locally for development and unit testing.

    Example::

        from scheduler_worker import BaseWorker, WorkerDevKit, task_handler

        class MyWorker(BaseWorker):
            capability = "echo"

            @task_handler
            async def handle(self, task_id, input_data):
                return {"echo": input_data}

        async def test_my_worker():
            kit = WorkerDevKit(MyWorker())
            result = await kit.run_task("task-1", {"msg": "hello"})
            assert result["echo"]["msg"] == "hello"
    """

    def __init__(self, worker: Any) -> None:
        self._worker = worker

    async def run_task(
        self,
        task_id: str,
        input_data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute one task directly in-process and return the result."""
        await self._worker.on_start()
        try:
            result = await self._worker.handle(task_id, input_data or {})
            await self._worker.on_task_success(task_id, result)
            return {"task_id": task_id, "status": "completed", "output": result}
        except Exception as exc:
            await self._worker.on_task_failure(task_id, exc)
            return {"task_id": task_id, "status": "failed", "error": str(exc)}
        finally:
            await self._worker.on_stop()

    async def run_batch(
        self,
        tasks: list[dict[str, Any]],
        *,
        concurrency: int = 4,
    ) -> list[dict[str, Any]]:
        """Run a batch of tasks with bounded concurrency. Useful for load testing."""
        sem = asyncio.Semaphore(concurrency)

        async def _one(t: dict) -> dict:
            async with sem:
                return await self.run_task(t["task_id"], t.get("input_data"))

        return await asyncio.gather(*[_one(t) for t in tasks])
