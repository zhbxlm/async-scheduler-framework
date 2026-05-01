"""Background task consumer loop."""

import asyncio
import logging
from datetime import datetime
from typing import Any, Callable

from async_scheduler.backends.base import LockBackend, LockHandle
from async_scheduler.core.models import ExecutionAttemptCreate, ExecutionAttemptStatus, Task, TaskStatus
from async_scheduler.executor import TaskExecutor
from async_scheduler.persistence import ExecutionAttemptRepository, TaskRepository, get_session, get_session_no_context
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
        lock_backend: LockBackend | None = None,
        worker_id: str | None = None,
        lease_ttl_seconds: float = 30.0,
        heartbeat_interval_seconds: float = 10.0,
    ) -> None:
        self._queue_manager = queue_manager
        self._executor = executor
        self._handler = handler
        self._max_concurrent_tasks = max_concurrent_tasks
        self._poll_interval = poll_interval
        self._running = False
        self._consumer_task: asyncio.Task[None] | None = None
        self._quota_manager = quota_manager
        self._completion_node = completion_node or TaskCompletionNode()
        self._lock_backend = lock_backend
        self._worker_id = worker_id or "local-worker"
        self._lease_ttl_seconds = lease_ttl_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds

    async def start(self) -> None:
        if self._running:
            logger.warning("Consumer is already running")
            return
        self._running = True
        self._consumer_task = asyncio.create_task(self._consumer_loop())
        logger.info("Task consumer started")

    async def stop(self) -> None:
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
        semaphore = asyncio.Semaphore(self._max_concurrent_tasks)

        async def _process_task(task: Task) -> None:
            async with semaphore:
                await self._process_single_task(task)

        while self._running:
            if self._executor.get_running_count() >= self._max_concurrent_tasks:
                await asyncio.sleep(self._poll_interval)
                continue
            claimed = await self._claim_next_task(timeout=1.0)
            if claimed is None:
                await asyncio.sleep(self._poll_interval)
                continue
            task, _, _ = claimed
            asyncio.create_task(_process_task(task))

    async def _process_next_once(self) -> bool:
        claimed = await self._claim_next_task(timeout=0.01)
        if claimed is None:
            return False
        task, handle, attempt_id = claimed
        await self._process_single_task(task, handle=handle, attempt_id=attempt_id)
        return True

    async def _claim_next_task(self, timeout: float | None = None) -> tuple[Task, LockHandle | None, str | None] | None:
        task = await self._queue_manager.dequeue(timeout=timeout)
        if task is None:
            return None
        if task.status == TaskStatus.CANCELLED:
            await self._update_task_status(task, TaskStatus.CANCELLED)
            return None

        if self._quota_manager is not None:
            try:
                self._quota_manager.release_queue(task.tenant_id)
                self._quota_manager.admit_running(task.tenant_id)
            except Exception:
                logger.exception("tenant quota rejected running task task_id=%s", task.id)
                await self._completion_node.finalize(task, TaskStatus.FAILED, error_message="tenant running quota exceeded")
                return None

        handle: LockHandle | None = None
        attempt_id: str | None = None
        if self._lock_backend is not None:
            handle = await self._lock_backend.acquire(f"task:{task.id}", ttl=self._lease_ttl_seconds, wait=0.0)
            if handle is None:
                await self._queue_manager.enqueue(task)
                return None
            async with get_session() as session:
                attempt = await ExecutionAttemptRepository.create(
                    session,
                    ExecutionAttemptCreate(
                        task_id=task.id,
                        worker_id=self._worker_id,
                        retry_index=task.retry_count,
                        lease_token=handle.token,
                    ),
                )
                attempt_id = attempt.id

        task.status = TaskStatus.RUNNING
        await self._update_task_status(task, TaskStatus.RUNNING)

        if attempt_id is not None:
            async with get_session() as session:
                await ExecutionAttemptRepository.update(
                    session,
                    attempt_id,
                    status=ExecutionAttemptStatus.RUNNING,
                    started_at=datetime.utcnow(),
                    last_heartbeat_at=datetime.utcnow(),
                )

        return task, handle, attempt_id

    async def _process_single_task(self, task: Task, handle: LockHandle | None = None, attempt_id: str | None = None) -> None:
        heartbeat_task: asyncio.Task[None] | None = None
        lease_lost = False
        try:
            if attempt_id is not None and handle is not None and self._lock_backend is not None:
                heartbeat_task = asyncio.create_task(self._heartbeat_loop(handle, attempt_id))

            async with await get_session_no_context() as session:
                fresh_task = await TaskRepository.get(session, task.id)
                if fresh_task and fresh_task.status == TaskStatus.CANCELLED:
                    return

            result = await self._executor.execute(task, self._handler)

            if heartbeat_task is not None and heartbeat_task.done():
                exc = heartbeat_task.exception()
                if exc is not None:
                    lease_lost = True

            if lease_lost:
                final_status = TaskStatus.FAILED
                error_message = "lease lost during execution"
                result_data = None
                attempt_status = ExecutionAttemptStatus.ABANDONED
            elif result.success:
                final_status = TaskStatus.SUCCESS
                error_message = None
                result_data = result.result
                attempt_status = ExecutionAttemptStatus.SUCCEEDED
            elif result.should_retry:
                final_status = TaskStatus.RETRY
                error_message = str(result.error) if result.error else "Unknown error"
                result_data = None
                attempt_status = ExecutionAttemptStatus.FAILED
            else:
                final_status = TaskStatus.FAILED
                error_message = str(result.error) if result.error else "Unknown error"
                result_data = None
                attempt_status = ExecutionAttemptStatus.FAILED

            await self._completion_node.finalize(task, final_status, error_message=error_message, result=result_data)

            if attempt_id is not None:
                async with get_session() as session:
                    await ExecutionAttemptRepository.finalize(
                        session,
                        attempt_id,
                        status=attempt_status,
                        error_message=error_message,
                        result_payload=result_data,
                    )

            if final_status == TaskStatus.RETRY:
                task.retry_count += 1
                await self._queue_manager.enqueue(task)

        except Exception as e:
            logger.error(f"Error processing task {task.id}: {e}", exc_info=True)
            await self._completion_node.finalize(task, TaskStatus.FAILED, error_message=str(e))
            if attempt_id is not None:
                async with get_session() as session:
                    await ExecutionAttemptRepository.finalize(
                        session,
                        attempt_id,
                        status=ExecutionAttemptStatus.FAILED,
                        error_message=str(e),
                    )
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except (asyncio.CancelledError, RuntimeError):
                    pass
            if handle is not None and self._lock_backend is not None:
                await self._lock_backend.release(handle)
            if self._quota_manager is not None:
                self._quota_manager.release_running(task.tenant_id)

    async def _heartbeat_loop(self, handle: LockHandle, attempt_id: str) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval_seconds)
            if self._lock_backend is not None:
                extended = await self._lock_backend.extend(handle, ttl=self._lease_ttl_seconds)
                if not extended:
                    raise RuntimeError(f"Lost lease for {handle.key}")
            async with get_session() as session:
                await ExecutionAttemptRepository.update(session, attempt_id, last_heartbeat_at=datetime.utcnow())

    async def _update_task_status(
        self,
        task: Task,
        status: TaskStatus,
        error_message: str | None = None,
        result: dict[str, Any] | None = None,
    ) -> None:
        async with get_session() as session:
            update_data = {"status": status, "updated_at": datetime.utcnow()}
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
        return self._running
