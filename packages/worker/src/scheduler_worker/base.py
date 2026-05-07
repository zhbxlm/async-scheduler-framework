"""scheduler_worker.base — BaseWorker and task_handler decorator.

Users subclass BaseWorker and decorate their handler with @task_handler.
No framework internals are exposed here.
"""
from __future__ import annotations

import asyncio
import functools
import logging
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)


def task_handler(fn: Callable) -> Callable:
    """Decorator that marks a method as the task handler.

    Wraps the method to ensure consistent error handling and logging.
    Supports both sync and async methods.
    """
    @functools.wraps(fn)
    async def wrapper(self: "BaseWorker", task_id: str, input_data: dict) -> dict:
        logger.debug("Worker %s: handling task_id=%s", self.capability, task_id)
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
    wrapper._is_task_handler = True
    return wrapper


class BaseWorker:
    """Base class for all async-scheduler Workers.

    Subclass this and implement ``handle()``, then register with the scheduler.

    Example::

        class ImageResizeWorker(BaseWorker):
            capability = "image_resize"

            @task_handler
            async def handle(self, task_id: str, input_data: dict) -> dict:
                url = input_data["url"]
                width = input_data.get("width", 800)
                # ... resize logic ...
                return {"output_url": "...", "width": width}

    The ``capability`` class attribute tells the scheduler which task type
    this worker handles. It must be unique across all workers in a cluster.
    """

    #: The task type / capability name this worker handles.
    #: Must be set on subclasses.
    capability: str = ""

    def __init__(self) -> None:
        if not self.capability:
            raise ValueError(
                f"{type(self).__name__} must define a non-empty 'capability' class attribute"
            )

    async def handle(self, task_id: str, input_data: dict[str, Any]) -> dict[str, Any]:
        """Process one task. Override this method (or use @task_handler on a method).

        Args:
            task_id:    Unique task identifier.
            input_data: Arbitrary input payload submitted with the task.

        Returns:
            Output dict that will be stored as the task result.

        Raises:
            Any exception will mark the task as failed.
        """
        raise NotImplementedError(
            f"{type(self).__name__}.handle() is not implemented. "
            "Implement it or decorate a method with @task_handler."
        )

    async def on_start(self) -> None:
        """Called once when the worker starts. Override for setup logic."""

    async def on_stop(self) -> None:
        """Called once when the worker shuts down. Override for cleanup logic."""

    async def on_task_success(self, task_id: str, result: dict[str, Any]) -> None:
        """Hook called after every successful task. Override for custom metrics/logging."""

    async def on_task_failure(self, task_id: str, exc: Exception) -> None:
        """Hook called after every failed task. Override for custom alerting."""
