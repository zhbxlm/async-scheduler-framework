"""scheduler_worker.devkit — run Workers locally without any infrastructure."""
from __future__ import annotations

import asyncio
from typing import Any


class WorkerDevKit:
    """Run a Worker in-process for development and unit testing.

    Example::

        kit = WorkerDevKit(ImageResizeWorker())
        result = await kit.run_task("t-1", {"url": "https://..."})
        assert result["status"] == "completed"
    """

    def __init__(self, worker: Any) -> None:
        self._worker = worker

    async def run_task(self, task_id: str, input_data: dict[str, Any] | None = None) -> dict[str, Any]:
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

    async def run_batch(self, tasks: list[dict[str, Any]], *, concurrency: int = 4) -> list[dict[str, Any]]:
        sem = asyncio.Semaphore(concurrency)
        async def _one(t: dict) -> dict:
            async with sem:
                return await self.run_task(t["task_id"], t.get("input_data"))
        return await asyncio.gather(*[_one(t) for t in tasks])
