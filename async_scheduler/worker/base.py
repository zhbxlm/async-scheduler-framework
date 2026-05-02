"""Worker abstraction for task execution."""

from __future__ import annotations

import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from typing import Any

from async_scheduler.core.models import Task, TaskStatus
from async_scheduler.executor import TaskExecutor
from async_scheduler.persistence import TaskRepository, get_session, get_session_no_context
from async_scheduler.queue import QueueManager

logger = logging.getLogger(__name__)


class Worker(ABC):
    """Abstract base class for workers."""

    def __init__(
        self,
        name: str,
        worker_id: str | None = None,
    ) -> None:
        """Initialize the worker."""
        self.name = name
        self.worker_id = worker_id or f"{name}-{uuid.uuid4().hex[:12]}"
        self._running = False
        self._task: asyncio.Task[None] | None = None

    @abstractmethod
    async def process(self, payload: dict[str, Any]) -> Any:
        """Process a task payload and return the result."""

    async def start(self) -> None:
        """Start the worker."""
        if self._running:
            logger.warning(f"Worker {self.worker_id} is already running")
            return

        self._running = True
        logger.info(f"Worker {self.worker_id} started")

    async def stop(self) -> None:
        """Stop the worker."""
        if not self._running:
            return

        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info(f"Worker {self.worker_id} stopped")

    def is_running(self) -> bool:
        """Check if the worker is running."""
        return self._running


class TaskWorker(Worker):
    """Worker that processes tasks from the queue."""

    def __init__(
        self,
        name: str,
        queue_manager: QueueManager,
        executor: TaskExecutor,
        max_concurrent_tasks: int = 5,
    ) -> None:
        """Initialize the task worker."""
        super().__init__(name)
        self._queue_manager = queue_manager
        self._executor = executor
        self._max_concurrent_tasks = max_concurrent_tasks

    async def process(self, payload: dict[str, Any]) -> Any:
        """Process a task payload."""
        return {"status": "processed", "payload": payload}

    async def start(self) -> None:
        """Start the task worker."""
        await super().start()
        self._task = asyncio.create_task(self._worker_loop())

    async def _worker_loop(self) -> None:
        """Main worker loop that processes tasks from the queue."""
        semaphore = asyncio.Semaphore(self._max_concurrent_tasks)

        async def _process_task(task: Task) -> None:
            async with semaphore:
                await self._process_single_task(task)

        while self._running:
            try:
                # Get next task
                task = await self._queue_manager.dequeue(timeout=1.0)

                if task is None:
                    await asyncio.sleep(0.5)
                    continue

                # Update status to running
                task.status = TaskStatus.RUNNING
                async with get_session() as session:
                    await TaskRepository.update(session, task.id, status=TaskStatus.RUNNING)

                # Process task
                asyncio.create_task(_process_task(task))

            except Exception as e:
                logger.error(f"Error in worker loop: {e}", exc_info=True)
                await asyncio.sleep(0.5)

    async def _process_single_task(self, task: Task) -> None:
        """Process a single task."""
        try:
            # Re-fetch to ensure not cancelled
            async with await get_session_no_context() as session:
                fresh_task = await TaskRepository.get(session, task.id)
                if fresh_task and fresh_task.status == TaskStatus.CANCELLED:
                    return

            # Execute with retry handling
            result = await self._executor.execute(task, self.process)

            # TaskExecutor already consumes the full retry budget internally.
            # When execute() returns a failed result here, the task should
            # converge to FAILED instead of being requeued a second time.
            if result.success:
                await self._update_task_status(task, TaskStatus.SUCCESS, result=result.result)
            else:
                await self._update_task_status(task, TaskStatus.FAILED, error=str(result.error))

        except Exception as e:
            logger.error(f"Error processing task {task.id}: {e}", exc_info=True)
            await self._update_task_status(task, TaskStatus.FAILED, error=str(e))

    async def _update_task_status(
        self,
        task: Task,
        status: TaskStatus,
        error: str | None = None,
        result: dict[str, Any] | None = None,
    ) -> None:
        """Update task status in database."""
        async with get_session() as session:
            update_data = {"status": status, "updated_at": task.updated_at, "retry_count": task.retry_count}

            if status == TaskStatus.RUNNING:
                update_data["started_at"] = task.started_at

            if status in (TaskStatus.SUCCESS, TaskStatus.FAILED):
                update_data["completed_at"] = task.completed_at

            if error:
                update_data["error_message"] = error

            if result:
                update_data["result"] = result

            await TaskRepository.update(session, task.id, **update_data)


class WorkerPool:
    """Pool of workers for parallel task processing."""

    def __init__(self, workers: list[TaskWorker]) -> None:
        """Initialize the worker pool."""
        self.workers = workers

    async def start(self) -> None:
        """Start all workers in the pool."""
        for worker in self.workers:
            await worker.start()

    async def stop(self) -> None:
        """Stop all workers in the pool."""
        for worker in self.workers:
            await worker.stop()

    def is_running(self) -> bool:
        """Check if all workers are running."""
        return all(w.is_running() for w in self.workers)

    def get_running_count(self) -> int:
        """Get the count of running workers."""
        return sum(1 for w in self.workers if w.is_running())
