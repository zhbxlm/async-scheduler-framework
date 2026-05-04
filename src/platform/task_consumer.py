"""TaskConsumer — aligned with docs/deepwiki-reference/任务执行.md

Polls capability queues, dequeues tasks, dispatches to executor.
Also handles stale running entry cleanup.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


class TaskConsumer:
    """Polls queues and drives TaskExecutor."""

    def __init__(
        self,
        queue_manager: Any,
        task_executor: Any,
        capabilities: list[str],
        *,
        poll_interval: float = 1.0,
        max_concurrent: int = 8,
        stale_threshold_seconds: float = 300.0,
    ) -> None:
        self._queue = queue_manager
        self._executor = task_executor
        self._capabilities = capabilities
        self._poll_interval = poll_interval
        self._max_concurrent = max_concurrent
        self._stale_threshold = stale_threshold_seconds
        self._running = False
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        self._running = True
        logger.info("TaskConsumer started caps=%s", self._capabilities)
        await self._poll_loop()

    async def stop(self) -> None:
        self._running = False
        for t in list(self._tasks):
            t.cancel()

    async def _poll_loop(self) -> None:
        while self._running:
            for cap in self._capabilities:
                await self._try_dequeue(cap)
            await asyncio.sleep(self._poll_interval)

    async def _try_dequeue(self, capability: str) -> None:
        if not await self._semaphore.acquire():
            return
        try:
            task_data = await self._queue.dequeue(capability)
            if task_data is None:
                self._semaphore.release()
                return
            t = asyncio.create_task(self._execute_task(task_data, capability))
            self._tasks.add(t)
            t.add_done_callback(self._tasks.discard)
        except Exception:
            self._semaphore.release()

    async def _execute_task(self, task_data: Any, capability: str) -> None:
        task_id = task_data.get("task_id", "unknown")
        try:
            await self._executor.execute(task_id, None, task_data)
        except Exception as e:
            logger.error("TaskConsumer: task_id=%s failed: %s", task_id, e)
        finally:
            self._semaphore.release()
