"""Task reconciler with distributed repair support."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable

from async_scheduler.backends.base import LockBackend, LockHandle
from async_scheduler.core.models import ExecutionAttemptStatus, Task, TaskStatus
from async_scheduler.distributed.worker_registry import WorkerRegistry
from async_scheduler.persistence import (
    ExecutionAttemptRepository,
    TaskRepository,
    get_session,
    get_session_no_context,
)
from async_scheduler.queue import QueueManager

logger = logging.getLogger(__name__)


class RepairStrategy(str, Enum):
    MARK_FAILED = "mark_failed"
    REQUEUE = "requeue"
    IGNORE = "ignore"


@dataclass
class ReconciliationMetrics:
    total_runs: int = 0
    stuck_tasks_found: int = 0
    tasks_repaired: int = 0
    tasks_ignored: int = 0
    tasks_requeued: int = 0
    last_run_at: datetime | None = None
    last_repaired_count: int = 0

    def get_repair_rate(self) -> float:
        if self.stuck_tasks_found == 0:
            return 0.0
        return (self.tasks_repaired / self.stuck_tasks_found) * 100


@dataclass
class ReconciliationConfig:
    stuck_after_seconds: int = 3600
    interval_seconds: int = 300
    max_tasks_per_run: int = 1000
    repair_strategy: RepairStrategy = RepairStrategy.MARK_FAILED
    check_timeouts: bool = True
    check_orphaned: bool = True


class TaskReconciler:
    def __init__(
        self,
        config: ReconciliationConfig | None = None,
        completion_node: Any | None = None,
        queue_manager: QueueManager | None = None,
        lock_backend: LockBackend | None = None,
        worker_registry: WorkerRegistry | None = None,
    ) -> None:
        self.config = config or ReconciliationConfig()
        self.completion_node = completion_node
        self.queue_manager = queue_manager
        self.lock_backend = lock_backend
        self.worker_registry = worker_registry
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._metrics = ReconciliationMetrics()
        self._reconciliation_handlers: list[Callable[[dict[str, Any]], None]] = []

    async def reconcile(self) -> int:
        repair_lock: LockHandle | None = None
        if self.lock_backend is not None:
            repair_lock = await self.lock_backend.acquire("reconciler:repair", ttl=5.0, wait=0.0)
            if repair_lock is None:
                return 0

        try:
            self._metrics.total_runs += 1
            self._metrics.last_run_at = datetime.utcnow()
            repaired = 0
            stuck_tasks: list[Task] = []
            threshold = datetime.utcnow() - timedelta(seconds=self.config.stuck_after_seconds)

            async with await get_session_no_context() as session:
                if self.config.check_timeouts:
                    running = await TaskRepository.list_all(
                        session, status=TaskStatus.RUNNING, limit=self.config.max_tasks_per_run
                    )
                    for task in running:
                        if await self._is_repairable_running_task(session, task, threshold):
                            stuck_tasks.append(task)

                if self.config.check_orphaned:
                    queued = await TaskRepository.list_all(
                        session, status=TaskStatus.QUEUED, limit=self.config.max_tasks_per_run
                    )
                    for task in queued:
                        updated_at = task.updated_at or task.created_at
                        if updated_at <= threshold:
                            stuck_tasks.append(task)

            self._metrics.stuck_tasks_found += len(stuck_tasks)

            for task in stuck_tasks:
                if repaired >= self.config.max_tasks_per_run:
                    break
                result = await self._repair_task(task)
                if result:
                    repaired += 1

            self._metrics.last_repaired_count = repaired

            for handler in self._reconciliation_handlers:
                try:
                    handler({"repaired": repaired, "stuck_found": len(stuck_tasks)})
                except Exception:
                    logger.exception("Reconciliation handler failed")

            return repaired
        finally:
            if repair_lock is not None and self.lock_backend is not None:
                await self.lock_backend.release(repair_lock)

    async def _is_repairable_running_task(self, session, task: Task, threshold: datetime) -> bool:
        started_at = task.started_at or task.updated_at or task.created_at
        if started_at > threshold:
            return False

        latest_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)
        if latest_attempt is None:
            return True

        worker_live = False
        if self.worker_registry is not None:
            worker_live = await self.worker_registry.is_live(latest_attempt.worker_id)

        lease_live = False
        if self.lock_backend is not None:
            lease_live = await self.lock_backend.is_locked(f"task:{task.id}")

        return not worker_live and not lease_live

    async def _repair_task(self, task: Task) -> bool:
        error_message = f"reconciler: task considered stuck after {self.config.stuck_after_seconds} seconds"

        if self.config.repair_strategy == RepairStrategy.IGNORE:
            self._metrics.tasks_ignored += 1
            return False

        async with get_session() as session:
            latest_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

            if self.config.repair_strategy == RepairStrategy.MARK_FAILED:
                await TaskRepository.update(
                    session,
                    task.id,
                    status=TaskStatus.FAILED,
                    error_message=error_message,
                    completed_at=datetime.utcnow(),
                )
                if latest_attempt is not None:
                    await ExecutionAttemptRepository.finalize(
                        session,
                        latest_attempt.id,
                        status=ExecutionAttemptStatus.ABANDONED,
                        error_message=error_message,
                    )
                self._metrics.tasks_repaired += 1
                return True

            if self.config.repair_strategy == RepairStrategy.REQUEUE:
                await TaskRepository.update(
                    session,
                    task.id,
                    status=TaskStatus.QUEUED,
                    error_message=error_message,
                    started_at=None,
                    retry_count=task.retry_count + 1,
                )
                if latest_attempt is not None:
                    await ExecutionAttemptRepository.finalize(
                        session,
                        latest_attempt.id,
                        status=ExecutionAttemptStatus.ABANDONED,
                        error_message=error_message,
                    )
                if self.queue_manager is not None:
                    refreshed = await TaskRepository.get(session, task.id)
                    if refreshed is not None and self.queue_manager.get_queue_count() == 0:
                        await self.queue_manager.enqueue(refreshed)
                self._metrics.tasks_requeued += 1
                return True

        return False

    async def start(self) -> None:
        if self._running:
            logger.warning("Reconciler is already running")
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        while self._running:
            try:
                await self.reconcile()
                await asyncio.sleep(self.config.interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in reconciliation loop: %s", e, exc_info=True)
                await asyncio.sleep(self.config.interval_seconds)

    def register_handler(self, handler: Callable[[dict[str, Any]], None]) -> None:
        self._reconciliation_handlers.append(handler)

    def unregister_handler(self, handler: Callable[[dict[str, Any]], None]) -> bool:
        try:
            self._reconciliation_handlers.remove(handler)
            return True
        except ValueError:
            return False

    def is_running(self) -> bool:
        return self._running

    def get_metrics(self) -> ReconciliationMetrics:
        return self._metrics

    def reset_metrics(self) -> None:
        self._metrics = ReconciliationMetrics()

    def update_config(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
