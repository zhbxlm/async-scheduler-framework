"""Background task consumer loop."""

import asyncio
import logging
from datetime import datetime
from typing import Any, Callable

from async_scheduler.core.models import Task, TaskStatus
from async_scheduler.executor import ExecutionResult, TaskExecutor
from async_scheduler.persistence import get_session_no_context, TaskRepository
from async_scheduler.platform.completion import TaskCompletionNode
from async_scheduler.platform.quota import TenantQuotaManager
from async_scheduler.queue import QueueManager

logger = logging.getLogger(__name__)


class TaskConsumer:
    """Consumes tasks from the queue and executes them."""

    def __init__(
        self,
        queue_manager: QueueManager,
        executor: TaskExecutor,
        handler: Callable[[dict[str, Any]], Any],
        max_concurrent_tasks: int = 10,
        poll_interval: float = 1.0,
        quota_manager: TenantQuotaManager | None = None,
        completion_node: TaskCompletionNode | None = None,
    ) -> None:
        """Initialize the task consumer."""
        self._queue_manager = queue_manager
        self._executor = executor
        self._handler = handler
        self._max_concurrent_tasks = max_concurrent_tasks
        self._poll_interval = poll_interval
        self._running = False
        self._consumer_task: asyncio.Task[None] | None = None
        self._quota_manager = quota_manager
        self._completion_node = completion_node or TaskCompletionNode()

    async def start(self) -> None:
        """Start the consumer loop."""
        if self._running:
            logger.warning("Consumer is already running")
            return

        self._running = True
        self._consumer_task = asyncio.create_task(self._consumer_loop())
        logger.info("Task consumer started")

    async def stop(self) -> None:
        """Stop the consumer loop gracefully."""
        if not self._running:
            return

        self._running = False

        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass

        logger.info("Task consumer stopped")

    async def _consumer_loop(self) -> None:
        """Main consumer loop."""
        semaphore = asyncio.Semaphore(self._max_concurrent_tasks)

        async def _process_task(task: Task) -> None:
            async with semaphore:
                await self._process_single_task(task)

        while self._running:
            # Check if we have capacity
            if self._executor.get_running_count() >= self._max_concurrent_tasks:
                await asyncio.sleep(self._poll_interval)
                continue

            # Get next task from queue
            task = await self._queue_manager.dequeue(timeout=1.0)

            if task is None:
                await asyncio.sleep(self._poll_interval)
                continue

            # Check if task was cancelled while in queue
            if task.status == TaskStatus.CANCELLED:
                await self._update_task_status(task, TaskStatus.CANCELLED)
                continue

            if self._quota_manager is not None:
                try:
                    self._quota_manager.release_queue(task.tenant_id)
                    self._quota_manager.admit_running(task.tenant_id)
                except Exception:
                    logger.exception("tenant quota rejected running task task_id=%s", task.id)
                    await self._completion_node.finalize(task, TaskStatus.FAILED, error_message="tenant running quota exceeded")
                    continue

            # Update status to running
            task.status = TaskStatus.RUNNING
            await self._update_task_status(task, TaskStatus.RUNNING)

            # Process task in background
            asyncio.create_task(_process_task(task))

    async def _process_single_task(self, task: Task) -> None:
        """Process a single task."""
        try:
            # Re-fetch task to ensure it's not cancelled
            async with await get_session_no_context() as session:
                fresh_task = await TaskRepository.get(session, task.id)
                if fresh_task and fresh_task.status == TaskStatus.CANCELLED:
                    return

            # Execute task
            result = await self._executor.execute(task, self._handler)

            # Determine final status
            if result.success:
                final_status = TaskStatus.SUCCESS
                error_message = None
                result_data = result.result
            elif result.should_retry:
                final_status = TaskStatus.RETRY
                error_message = str(result.error) if result.error else "Unknown error"
                result_data = None
            else:
                final_status = TaskStatus.FAILED
                error_message = str(result.error) if result.error else "Unknown error"
                result_data = None

            await self._completion_node.finalize(
                task,
                final_status,
                error_message=error_message,
                result=result_data,
            )

            # If retry, re-queue the task
            if final_status == TaskStatus.RETRY:
                task.retry_count += 1
                await self._queue_manager.enqueue(task)

        except Exception as e:
            logger.error(f"Error processing task {task.id}: {e}", exc_info=True)
            await self._completion_node.finalize(
                task,
                TaskStatus.FAILED,
                error_message=str(e),
            )
        finally:
            if self._quota_manager is not None:
                self._quota_manager.release_running(task.tenant_id)

    async def _update_task_status(
        self,
        task: Task,
        status: TaskStatus,
        error_message: str | None = None,
        result: dict[str, Any] | None = None,
    ) -> None:
        """Update task status in database."""
        async with await get_session_no_context() as session:
            update_data = {
                "status": status,
                "updated_at": datetime.utcnow(),
            }

            if status == TaskStatus.RUNNING:
                update_data["started_at"] = datetime.utcnow()

            if status in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT):
                update_data["completed_at"] = datetime.utcnow()

            if error_message:
                update_data["error_message"] = error_message

            if result:
                update_data["result"] = result

            await TaskRepository.update(session, task.id, **update_data)

    def is_running(self) -> bool:
        """Check if the consumer is running."""
        return self._running
