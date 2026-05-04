"""Task completion node with idempotent finalization semantics."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from async_scheduler.backends.base import CompletionDedupBackend
from async_scheduler.core.models import ExecutionAttemptStatus, Task, TaskStatus
from async_scheduler.persistence import ExecutionAttemptRepository, TaskRepository, get_session, get_session_no_context
from async_scheduler.platform.async_proxy import AsyncProxySidecar, TaskEvent
from async_scheduler.platform.callback import CallbackDispatcher

logger = logging.getLogger(__name__)


@dataclass
class CompletionMetrics:
    total_completed: int = 0
    successful_completions: int = 0
    failed_completions: int = 0
    cancelled_completions: int = 0
    timeout_completions: int = 0
    callback_dispatches: int = 0
    callback_failures: int = 0

    def get_success_rate(self) -> float:
        if self.total_completed == 0:
            return 0.0
        return (self.successful_completions / self.total_completed) * 100

    def get_callback_success_rate(self) -> float:
        if self.callback_dispatches == 0:
            return 0.0
        return ((self.callback_dispatches - self.callback_failures) / self.callback_dispatches) * 100


class _InMemoryCompletionDedupBackend(CompletionDedupBackend):
    def __init__(self) -> None:
        self._claims: set[str] = set()

    async def claim_once(self, key: str, ttl_seconds: float | None = None) -> bool:
        if key in self._claims:
            return False
        self._claims.add(key)
        return True

    async def clear(self) -> None:
        self._claims.clear()


class TaskCompletionNode:
    TERMINAL_STATUSES = {
        TaskStatus.SUCCESS,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.TIMEOUT,
    }

    def __init__(
        self,
        callback_dispatcher: CallbackDispatcher | None = None,
        enable_metrics: bool = True,
        dedup_backend: CompletionDedupBackend | None = None,
        dedup_ttl_seconds: float = 86400.0,
        async_proxy_sidecar: AsyncProxySidecar | None = None,
    ) -> None:
        self.callback_dispatcher = callback_dispatcher or CallbackDispatcher()
        self.async_proxy_sidecar = async_proxy_sidecar
        self._metrics = CompletionMetrics() if enable_metrics else None
        self._enable_metrics = enable_metrics
        self._completion_handlers: dict[TaskStatus, list[Callable[[Task], None]]] = defaultdict(list)
        self._dedup_backend = dedup_backend or _InMemoryCompletionDedupBackend()
        self._dedup_ttl_seconds = dedup_ttl_seconds

    async def finalize(
        self,
        task: Task,
        status: TaskStatus,
        *,
        error_message: str | None = None,
        result: dict[str, Any] | None = None,
        finalize_latest_attempt: bool = False,
    ) -> Task | None:
        completion_key = f"{task.id}:{status.value}"

        async with await get_session_no_context() as session:
            stored = await TaskRepository.get(session, task.id)
        if stored is None:
            logger.warning("Failed to load task %s for completion", task.id)
            return None

        try:
            claimed = await self._dedup_backend.claim_once(completion_key, ttl_seconds=self._dedup_ttl_seconds)
        except Exception as exc:
            logger.warning(
                "Completion dedupe unavailable for task %s status %s; proceeding best-effort: %s",
                task.id,
                status.value,
                exc,
            )
            claimed = True
        if not claimed:
            return stored

        if stored.status in self.TERMINAL_STATUSES and stored.status == status:
            return stored

        completed_at = None
        started_at = None
        if status == TaskStatus.RUNNING:
            started_at = datetime.utcnow()
        if status in self.TERMINAL_STATUSES:
            completed_at = datetime.utcnow()

        async with get_session() as session:
            updated = await TaskRepository.update(
                session,
                task.id,
                status=status,
                updated_at=datetime.utcnow(),
                started_at=started_at,
                completed_at=completed_at,
                error=error_message,
                result=result,
            )
            if updated is not None and finalize_latest_attempt:
                latest_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)
                attempt_status = self._map_attempt_status(status)
                if latest_attempt is not None and attempt_status is not None:
                    # Completion wins over reconciler's ABANDONED (reconciler is a best-effort
                    # repair; actual completion has higher authority).  However, we must not
                    # override a SUCCEEDED/FAILED/CANCELLED that was already written by a
                    # concurrent completion path.
                    non_overridable = {
                        ExecutionAttemptStatus.SUCCEEDED,
                        ExecutionAttemptStatus.FAILED,
                        ExecutionAttemptStatus.CANCELLED,
                    }
                    if latest_attempt.status not in non_overridable:
                        await ExecutionAttemptRepository.finalize(
                            session,
                            latest_attempt.id,
                            status=attempt_status,
                            error_message=error_message,
                        )

        if not updated:
            logger.warning("Failed to persist final state for task %s", task.id)
            return None

        if self._enable_metrics:
            self._metrics.total_completed += 1
            if status == TaskStatus.SUCCESS:
                self._metrics.successful_completions += 1
            elif status == TaskStatus.FAILED:
                self._metrics.failed_completions += 1
            elif status == TaskStatus.CANCELLED:
                self._metrics.cancelled_completions += 1
            elif status == TaskStatus.TIMEOUT:
                self._metrics.timeout_completions += 1

        callback_delivery: str | None = None
        # source callback_url from Task field
        _callback_url: str | None = getattr(updated, "callback_url", None)
        if status in (TaskStatus.SUCCESS, TaskStatus.FAILED) and _callback_url:
            callback_ok = await self._dispatch_callback(updated, status, result, error_message, callback_url=_callback_url)
            callback_delivery = "ok" if callback_ok else "failed"
            callback_delivery = "delivered" if callback_ok else "failed"

        await self._publish_async_proxy_event(updated, status, result, error_message, callback_delivery=callback_delivery)

        for handler in self._completion_handlers[status]:
            try:
                handler(updated)
            except Exception:
                logger.exception("Completion handler failed for task %s", task.id)

        return updated

    def _map_attempt_status(self, status: TaskStatus) -> ExecutionAttemptStatus | None:
        if status == TaskStatus.SUCCESS:
            return ExecutionAttemptStatus.SUCCEEDED
        if status in {TaskStatus.FAILED, TaskStatus.TIMEOUT, TaskStatus.RETRY}:
            return ExecutionAttemptStatus.FAILED
        if status == TaskStatus.CANCELLED:
            return ExecutionAttemptStatus.CANCELLED
        return None

    async def _dispatch_callback(
        self,
        task: Task,
        status: TaskStatus,
        result: dict[str, Any] | None,
        error_message: str | None,
        callback_url: str | None = None,
    ) -> bool:
        _url = callback_url or getattr(task, "callback_url", None)
        try:
            if self._enable_metrics:
                self._metrics.callback_dispatches += 1
            ok = await self.callback_dispatcher.dispatch(
                _url,
                {
                    "task_id": task.id,
                    "name": task.name,
                    "status": status.value,
                    "result": result,
                    "error": error_message,
                    "completed_at": task.completed_at.isoformat() if task.completed_at else None,
                },
            )
            return bool(ok)
        except Exception as e:
            logger.error("Failed to dispatch callback for task %s: %s", task.id, e, exc_info=True)
            if self._enable_metrics:
                self._metrics.callback_failures += 1
            return False

    async def _publish_async_proxy_event(
        self,
        task: Task,
        status: TaskStatus,
        result: dict[str, Any] | None,
        error_message: str | None,
        *,
        callback_delivery: str | None = None,
    ) -> None:
        if self.async_proxy_sidecar is None:
            return
        try:
            completion_kind = status.value
            if error_message == "lease lost during execution":
                completion_kind = "lease_lost"

            # 尝试获取 callback retry/dead-letter 统计（如果有 dispatcher）
            retry_queue_size = 0
            dead_letter_size = 0
            if self.callback_dispatcher is not None:
                try:
                    stats = await self.callback_dispatcher.get_stats()
                    retry_queue_size = stats.get("retry_queue_size", 0)
                    dead_letter_size = stats.get("dead_letter_size", 0)
                except Exception:
                    pass
            await self.async_proxy_sidecar.publish(
                TaskEvent(
                    task_id=task.id,
                    status=status.value,
                    result=result,
                    error=error_message,
                    task_name=task.name,
                    tenant_id=task.tenant_id,
                    callback_url=task.callback_url,
                    completed_at=task.completed_at.timestamp() if task.completed_at else None,
                    completion_kind=completion_kind,
                    callback_delivery=callback_delivery,
                    callback_retry_queue_size=retry_queue_size,
                    callback_dead_letter_size=dead_letter_size,
                )
            )
        except Exception:
            logger.exception("Failed to publish async proxy event for task %s", task.id)

    def register_completion_handler(self, status: TaskStatus, handler: Callable[[Task], None]) -> None:
        self._completion_handlers[status].append(handler)

    def unregister_completion_handler(self, status: TaskStatus, handler: Callable[[Task], None]) -> bool:
        try:
            self._completion_handlers[status].remove(handler)
            return True
        except ValueError:
            return False

    def get_metrics(self) -> CompletionMetrics | None:
        return self._metrics

    def reset_metrics(self) -> None:
        if self._metrics:
            self._metrics = CompletionMetrics()
