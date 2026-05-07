"""scheduler_worker.base — BaseWorker and @task_handler decorator."""
from __future__ import annotations

import asyncio
import functools
import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


def task_handler(fn: Callable) -> Callable:
    """Mark a method as the task handler. Supports sync and async."""
    @functools.wraps(fn)
    async def wrapper(self: "BaseWorker", task_id: str, input_data: dict) -> dict:
        logger.debug("Worker %s: task_id=%s", self.capability, task_id)
        try:
            if asyncio.iscoroutinefunction(fn):
                result = await fn(self, task_id, input_data)
            else:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(None, fn, self, task_id, input_data)
            return result if isinstance(result, dict) else {"result": result}
        except Exception as exc:
            logger.error("Worker %s: task_id=%s failed: %s", self.capability, task_id, exc)
            raise
    wrapper._is_task_handler = True  # type: ignore[attr-defined]
    return wrapper


class BaseWorker:
    """Base class for all async-scheduler Workers.

    Subclass this, set ``capability``, implement ``handle()``.

    Example::

        class ImageResizeWorker(BaseWorker):
            capability = "image_resize"

            @task_handler
            async def handle(self, task_id: str, input_data: dict) -> dict:
                return {"output_url": "https://cdn.example.com/resized.jpg"}
    """

    capability: str = ""

    def __init__(self) -> None:
        if not self.capability:
            raise ValueError(f"{type(self).__name__} must define a non-empty 'capability'")

    async def handle(self, task_id: str, input_data: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError(f"{type(self).__name__}.handle() not implemented")

    async def on_start(self) -> None: ...
    async def on_stop(self) -> None: ...
    async def on_task_success(self, task_id: str, result: dict[str, Any]) -> None: ...
    async def on_task_failure(self, task_id: str, exc: Exception) -> None: ...
