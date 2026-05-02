"""Task reconciler with distributed repair support."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass
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
@dataclass
class RepairAuditEntry:
    task_id: str
    action: str
    strategy: str
    attempt_id: str | None
    attempt_status: str | None
    task_status: str | None
    timestamp: datetime
    error_message: str | None = None


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
        stale_ttl_seconds: int | None = None,  # P1-TODO-8: configurable stale TTL
    ) -> None:
        self.config = config or ReconciliationConfig()
        # P1-TODO-8: override stuck_after_seconds if stale_ttl_seconds provided
        if stale_ttl_seconds is not None:
            self.config = ReconciliationConfig(
                stuck_after_seconds=stale_ttl_seconds,
                interval_seconds=self.config.interval_seconds,
                max_tasks_per_run=self.config.max_tasks_per_run,
                repair_strategy=self.config.repair_strategy,
                check_timeouts=self.config.check_timeouts,
                check_orphaned=self.config.check_orphaned,
            )
        self.completion_node = completion_node
        self.queue_manager = queue_manager
        self.lock_backend = lock_backend
        self.worker_registry = worker_registry
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._metrics = ReconciliationMetrics()
        self._reconciliation_handlers: list[Callable[[dict[str, Any]], None]] = []
        self._repair_history: list[RepairAuditEntry] = []

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
            try:
                worker_live = await self.worker_registry.is_live(latest_attempt.worker_id)
            except Exception as exc:
                logger.warning(
                    "Worker liveness lookup failed for %s during reconcile; skipping repair for safety: %s",
                    latest_attempt.worker_id,
                    exc,
                )
                return False

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
                self._record_repair(
                    task_id=task.id,
                    action="mark_failed",
                    attempt_id=None if latest_attempt is None else latest_attempt.id,
                    attempt_status=ExecutionAttemptStatus.ABANDONED.value if latest_attempt is not None else None,
                    task_status=TaskStatus.FAILED.value,
                    error_message=error_message,
                )
                self._metrics.tasks_repaired += 1
                return True

            if self.config.repair_strategy == RepairStrategy.REQUEUE:
                refreshed = await TaskRepository.get(session, task.id)
                if refreshed is None:
                    return False

                if self.queue_manager is not None:
                    current_queue_count = await self.queue_manager.get_queue_count()
                    if current_queue_count == 0:
                        try:
                            await self.queue_manager.enqueue(refreshed)
                        except Exception as exc:
                            logger.warning(
                                "Failed to enqueue repaired task %s during reconcile; leaving task running for retry: %s",
                                task.id,
                                exc,
                            )
                            return False

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
                self._record_repair(
                    task_id=task.id,
                    action="requeue",
                    attempt_id=None if latest_attempt is None else latest_attempt.id,
                    attempt_status=ExecutionAttemptStatus.ABANDONED.value if latest_attempt is not None else None,
                    task_status=TaskStatus.QUEUED.value,
                    error_message=error_message,
                )
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

    def list_repair_history(
        self,
        limit: int = 100,
        offset: int = 0,
        *,
        action: str | None = None,
        task_id: str | None = None,
    ) -> list[dict[str, Any]]:
        items = self._repair_history
        if action is not None:
            items = [item for item in items if item.action == action]
        if task_id is not None:
            items = [item for item in items if item.task_id == task_id]
        items = items[offset : offset + limit]
        return [asdict(item) for item in items]

    def _record_repair(
        self,
        *,
        task_id: str,
        action: str,
        attempt_id: str | None,
        attempt_status: str | None,
        task_status: str | None,
        error_message: str | None,
    ) -> None:
        self._repair_history.insert(
            0,
            RepairAuditEntry(
                task_id=task_id,
                action=action,
                strategy=self.config.repair_strategy.value,
                attempt_id=attempt_id,
                attempt_status=attempt_status,
                task_status=task_status,
                timestamp=datetime.utcnow(),
                error_message=error_message,
            ),
        )
        if len(self._repair_history) > 1000:
            self._repair_history = self._repair_history[:1000]

    def reset_metrics(self) -> None:
        self._metrics = ReconciliationMetrics()
        self._repair_history = []

    def update_config(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)

    # ------------------------------------------------------------------
    # Three-phase reconciliation (deepwiki alignment)
    # ------------------------------------------------------------------

    async def reconcile_three_phase(
        self,
        *,
        callback_dispatcher: Any | None = None,
        throttle_mark_failed: int = 50,
    ) -> dict[str, int]:
        """Run all three reconciliation phases in sequence.

        Phase 1: Double-write reconciliation
            Ensure completed_at is populated for all terminal tasks (backfill
            any gaps caused by partial writes).

        Phase 2: Stuck task recovery
            Scan RUNNING/QUEUED tasks whose lease has expired and worker is
            dead; requeue or mark-failed with throttle control.

        Phase 3: Lost callback recovery
            For terminal tasks with a callback_url that has not been marked
            delivered, compensate by re-dispatching via callback_dispatcher.

        Returns a dict with per-phase counts.
        """
        stats: dict[str, int] = {
            "phase1_backfilled": 0,
            "phase2_requeued": 0,
            "phase2_mark_failed": 0,
            "phase3_callback_requeued": 0,
        }

        try:
            stats["phase1_backfilled"] = await self._phase1_double_write()
        except Exception:
            logger.exception("reconcile phase1 failed")

        try:
            p2 = await self._phase2_stuck_recovery(throttle_mark_failed=throttle_mark_failed)
            stats["phase2_requeued"] = p2["requeued"]
            stats["phase2_mark_failed"] = p2["mark_failed"]
        except Exception:
            logger.exception("reconcile phase2 failed")

        if callback_dispatcher is not None:
            try:
                stats["phase3_callback_requeued"] = await self._phase3_lost_callbacks(callback_dispatcher)
            except Exception:
                logger.exception("reconcile phase3 failed")

        return stats

    async def _phase1_double_write(self) -> int:
        """Ensure completed_at is populated for all terminal tasks."""
        terminal_statuses = [
            TaskStatus.SUCCESS,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.TIMEOUT,
        ]
        backfilled = 0
        async with await get_session_no_context() as session:
            for status in terminal_statuses:
                tasks = await TaskRepository.list_all(
                    session, status=status, limit=self.config.max_tasks_per_run
                )
                for task in tasks:
                    if task.completed_at is None:
                        try:
                            async with get_session() as ws:
                                await TaskRepository.update(
                                    ws,
                                    task.id,
                                    completed_at=task.updated_at or datetime.utcnow(),
                                )
                            backfilled += 1
                            logger.info(
                                "phase1: backfilled completed_at task=%s status=%s",
                                task.id,
                                task.status.value,
                            )
                        except Exception:
                            logger.exception("phase1: failed to backfill task %s", task.id)
        return backfilled

    async def _phase2_stuck_recovery(self, *, throttle_mark_failed: int = 50) -> dict[str, int]:
        """Detect stuck RUNNING/QUEUED tasks and repair them."""
        requeued = 0
        mark_failed = 0
        threshold = datetime.utcnow() - timedelta(seconds=self.config.stuck_after_seconds)

        async with await get_session_no_context() as session:
            running_tasks = await TaskRepository.list_all(
                session, status=TaskStatus.RUNNING, limit=self.config.max_tasks_per_run
            )
            queued_tasks = await TaskRepository.list_all(
                session, status=TaskStatus.QUEUED, limit=self.config.max_tasks_per_run
            )

        for task in running_tasks + queued_tasks:
            updated_at = task.updated_at or task.created_at
            if updated_at > threshold:
                continue

            if self.config.repair_strategy == RepairStrategy.REQUEUE and self.queue_manager is not None:
                # Open one session: check repairability, then requeue in same session
                async with await get_session_no_context() as session:
                    repairable = await self._is_repairable_running_task(session, task, threshold)
                if not repairable:
                    continue
                try:
                    await self.queue_manager.enqueue(task)
                    async with get_session() as session:
                        await TaskRepository.update(
                            session,
                            task.id,
                            status=TaskStatus.QUEUED,
                            error_message="reconciler-phase2: requeued after lease expiry",
                            started_at=None,
                            retry_count=(task.retry_count or 0) + 1,
                        )
                    requeued += 1
                    self._record_repair(
                        task_id=task.id,
                        action="phase2_requeue",
                        attempt_id=None,
                        attempt_status=ExecutionAttemptStatus.ABANDONED.value,
                        task_status=TaskStatus.QUEUED.value,
                        error_message="reconciler-phase2: requeued after lease expiry",
                    )
                except Exception:
                    logger.exception("phase2: requeue failed task=%s", task.id)
            else:
                if mark_failed >= throttle_mark_failed:
                    logger.warning("phase2: throttle reached; deferring remaining")
                    break
                error_msg = "reconciler-phase2: marked failed after lease expiry"
                try:
                    # One session: check repairability + get latest attempt + mark failed
                    async with get_session() as session:
                        repairable = await self._is_repairable_running_task(session, task, threshold)
                        if not repairable:
                            continue
                        latest = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)
                        await TaskRepository.update(
                            session,
                            task.id,
                            status=TaskStatus.FAILED,
                            error_message=error_msg,
                            completed_at=datetime.utcnow(),
                        )
                        if latest is not None:
                            await ExecutionAttemptRepository.finalize(
                                session,
                                latest.id,
                                status=ExecutionAttemptStatus.ABANDONED,
                                error_message=error_msg,
                            )
                    mark_failed += 1
                    self._record_repair(
                        task_id=task.id,
                        action="phase2_mark_failed",
                        attempt_id=None,
                        attempt_status=ExecutionAttemptStatus.ABANDONED.value,
                        task_status=TaskStatus.FAILED.value,
                        error_message=error_msg,
                    )
                except Exception:
                    logger.exception("phase2: mark_failed failed task=%s", task.id)

        return {"requeued": requeued, "mark_failed": mark_failed}

    async def _phase3_lost_callbacks(self, callback_dispatcher: Any) -> int:
        """Re-enqueue callbacks for terminal tasks never delivered."""
        requeued = 0
        terminal_statuses = [
            TaskStatus.SUCCESS,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.TIMEOUT,
        ]
        async with await get_session_no_context() as session:
            for status in terminal_statuses:
                tasks = await TaskRepository.list_all(
                    session, status=status, limit=self.config.max_tasks_per_run
                )
                for task in tasks:
                    if not getattr(task, "callback_url", None):
                        continue
                    if await callback_dispatcher.is_done(task.id):
                        continue
                    if await callback_dispatcher.is_in_retry_queue(task.id):
                        continue
                    try:
                        await callback_dispatcher.dispatch(
                            task.callback_url,
                            {
                                "task_id": task.id,
                                "status": task.status.value,
                                "result": getattr(task, "result", None),
                                "error_message": getattr(task, "error_message", None),
                            },
                            task_id=task.id,
                        )
                        requeued += 1
                        logger.info("phase3: compensated callback task=%s", task.id)
                    except Exception:
                        logger.exception("phase3: dispatch failed task=%s", task.id)
        return requeued
