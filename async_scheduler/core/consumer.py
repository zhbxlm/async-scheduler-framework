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
        # P0-TODO-2: round-robin capability index
        self._cap_idx: int = 0
        # P0-TODO-3: lease-lost event map
        self._lease_lost_events: dict[str, asyncio.Event] = {}

    async def start(self) -> None:
        if self._running:
            logger.warning("Consumer is already running")
            return
        self._running = True
        self._consumer_task = asyncio.create_task(self._consumer_loop())
        logger.info("Task consumer started worker_id=%s max_concurrent=%d poll_interval=%.1fs",
                    self._worker_id, self._max_concurrent_tasks, self._poll_interval)

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
        # P3/P4: cache capabilities + exponential backoff on idle
        _cap_cache: list[str] = []
        _cap_cache_ttl: float = 0.0
        _CAP_CACHE_SECONDS = 30.0  # refresh capability list every 30s
        _idle_streak: int = 0       # consecutive empty polls
        _MAX_BACKOFF: float = 8.0   # cap backoff at 8s

        import random, time as _time

        semaphore = asyncio.Semaphore(self._max_concurrent_tasks)

        async def _process_task(task: Task) -> None:
            async with semaphore:
                await self._process_single_task(task)

        while self._running:
            if self._executor.get_running_count() >= self._max_concurrent_tasks:
                await asyncio.sleep(self._poll_interval)
                continue

            # P3: refresh capability cache lazily (not every poll)
            now = _time.monotonic()
            if not _cap_cache or now >= _cap_cache_ttl:
                try:
                    _cap_cache = await self._queue_manager.discover_capabilities()
                    _cap_cache_ttl = now + _CAP_CACHE_SECONDS
                except Exception:
                    pass

            # Round-robin across known capabilities
            if _cap_cache:
                cap = _cap_cache[self._cap_idx % len(_cap_cache)]
                self._cap_idx += 1
                # Periodic per-capability debug logging
                if self._cap_idx % 50 == 0:
                    for c in _cap_cache:
                        try:
                            stats = await self._queue_manager.get_capability_stats(c)
                            logger.debug(
                                "cap=%s pending=%d running=%d",
                                c, stats.pending, stats.running,
                            )
                        except Exception:
                            pass
                claimed = await self._claim_next_task(timeout=0.05, capability=cap)
            else:
                claimed = await self._claim_next_task(timeout=1.0)

            if claimed is None:
                # P4: exponential backoff with jitter on empty polls
                _idle_streak += 1
                backoff = min(self._poll_interval * (2 ** min(_idle_streak - 1, 5)), _MAX_BACKOFF)
                jitter = random.uniform(0, backoff * 0.2)  # ±20% jitter
                await asyncio.sleep(backoff + jitter)
                continue

            _idle_streak = 0  # reset on successful claim
            task, _, _ = claimed
            asyncio.create_task(_process_task(task))

    async def _process_next_once(self) -> bool:
        claimed = await self._claim_next_task(timeout=0.01)
        if claimed is None:
            return False
        task, handle, attempt_id = claimed
        await self._process_single_task(task, handle=handle, attempt_id=attempt_id)
        return True

    async def _claim_next_task(self, timeout: float | None = None, capability: str = "default") -> tuple[Task, LockHandle | None, str | None] | None:
        task = await self._queue_manager.dequeue(timeout=timeout, capability=capability)
        if task is None:
            return None
        if task.status == TaskStatus.CANCELLED:
            await self._update_task_status(task, TaskStatus.CANCELLED)
            return None

        # G7: if task was in SCHEDULED state (just promoted from delayed queue),
        # update DB status to QUEUED now that it is actually being consumed.
        if task.status == TaskStatus.SCHEDULED:
            async with await get_session_no_context() as _session:
                task = await TaskRepository.update(_session, task.id, status=TaskStatus.QUEUED) or task

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
        # P0-TODO-3: per-task lease-lost event
        lease_lost_event = asyncio.Event()
        self._lease_lost_events[task.id] = lease_lost_event
        try:
            if attempt_id is not None and handle is not None and self._lock_backend is not None:
                heartbeat_task = asyncio.create_task(
                    self._heartbeat_loop(handle, attempt_id, task_id=task.id)
                )

            async with await get_session_no_context() as session:
                fresh_task = await TaskRepository.get(session, task.id)
                if fresh_task and fresh_task.status == TaskStatus.CANCELLED:
                    return

            result = await self._executor.execute(task, self._handler, lease_lost_event=lease_lost_event)

            if heartbeat_task is not None and heartbeat_task.done():
                exc = heartbeat_task.exception()
                if exc is not None:
                    lease_lost = True

            # Also check lease_lost_event
            if lease_lost_event.is_set():
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
            else:
                final_status = TaskStatus.FAILED
                error_message = str(result.error) if result.error else "Unknown error"
                result_data = None
                attempt_status = ExecutionAttemptStatus.FAILED

            await self._completion_node.finalize(
                task,
                final_status,
                error_message=error_message,
                result=result_data,
                finalize_latest_attempt=attempt_id is not None and attempt_status != ExecutionAttemptStatus.ABANDONED,
            )

            if attempt_id is not None and attempt_status == ExecutionAttemptStatus.ABANDONED:
                async with get_session() as session:
                    await ExecutionAttemptRepository.finalize(
                        session,
                        attempt_id,
                        status=attempt_status,
                        error_message=error_message,
                        result_payload=result_data,
                    )


        except Exception as e:
            logger.error(
                "task processing failed task_id=%s worker_id=%s tenant_id=%s error=%s",
                task.id, self._worker_id, task.tenant_id, type(e).__name__,
                exc_info=True,
            )
            await self._completion_node.finalize(
                task,
                TaskStatus.FAILED,
                error_message=str(e),
                finalize_latest_attempt=attempt_id is not None,
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
            # P0-TODO-3: clean up lease_lost_event
            self._lease_lost_events.pop(task.id, None)

    async def _heartbeat_loop(self, handle: LockHandle, attempt_id: str, task_id: str | None = None) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval_seconds)
            if self._lock_backend is not None:
                extended = await self._lock_backend.extend(handle, ttl=self._lease_ttl_seconds)
                if not extended:
                    # P0-TODO-3: signal via event instead of raising, so executor can interrupt
                    event = self._lease_lost_events.get(task_id) if task_id else None
                    if event is not None:
                        event.set()
                    else:
                        raise RuntimeError(f"Lost lease for {handle.key}")
                    return
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
