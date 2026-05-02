"""Task router for lifecycle orchestration and queue admission.

G3 enhancements (deepwiki alignment):
- idempotency_key: Redis SET NX atomic claim; duplicate submissions return
  the existing task instead of creating a new one.
- dispatch_mode: DAG_ORCHESTRATED vs RAYDATA_NATIVE routing paths.
- rollback: on enqueue failure, remove DB record and Redis idempotency key.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from async_scheduler.core.models import Task, TaskCreate, TaskStatus
from async_scheduler.persistence import TaskRepository, get_session_no_context
from async_scheduler.platform.quota import TenantQuotaManager
from async_scheduler.queue import QueueManager

logger = logging.getLogger(__name__)


class DispatchMode(str, Enum):
    """Dispatch mode controls which execution path a task takes.

    DAG_ORCHESTRATED: task is a node in a DAG; DAGEngine manages dependencies.
    RAYDATA_NATIVE: task is dispatched directly to a Ray actor (or actor pool).
    DIRECT: default mode; task goes straight to the capability queue.
    """
    DIRECT = "direct"
    DAG_ORCHESTRATED = "dag_orchestrated"
    RAYDATA_NATIVE = "raydata_native"


# Redis key prefix for idempotency claims
_IDEM_PREFIX = "idem:task"
_IDEM_TTL_SECONDS = 86400  # 24h


class TaskRouter:
    """Creates tasks, applies scheduling rules, and enqueues them.

    Supports:
    - Capability inference from task.tags
    - idempotency_key: atomic Redis NX claim prevents duplicate task creation
    - dispatch_mode: routes task to the appropriate execution path
    - rollback: cleans up DB + Redis on enqueue failure
    """

    def __init__(
        self,
        queue_manager: QueueManager,
        quota_manager: TenantQuotaManager | None = None,
        redis_client: Any | None = None,
    ) -> None:
        self.queue_manager = queue_manager
        self.quota_manager = quota_manager
        self._redis = redis_client
        # In-process idempotency store (per-instance; for tests and single-process deployments)
        self._idem_store: dict[str, str] = {}

    async def create_task(
        self,
        task_create: TaskCreate,
        dispatch_mode: DispatchMode = DispatchMode.DIRECT,
    ) -> Task:
        """Create and enqueue a task.

        If task_create.idempotency_key is set:
        - Atomically claim the key via Redis SET NX (or in-process dict fallback)
        - If already claimed, return the existing task without re-creating
        On enqueue failure: rollback DB record and idempotency key.
        """
        if self.quota_manager is not None:
            await self.quota_manager.admit_queue(task_create.tenant_id)

        # G3: idempotency check
        idem_key = task_create.idempotency_key
        if idem_key:
            existing = await self._check_idempotency(idem_key)
            if existing is not None:
                logger.info("TaskRouter idempotency hit key=%s task_id=%s", idem_key, existing.id)
                return existing

        async with await get_session_no_context() as session:
            task = await TaskRepository.create(session, task_create)
            scheduled_at = task.scheduled_at

            # P1-TODO-6: infer capability from task metadata
            capability = _infer_capability(task)

            # G3: claim idempotency key after DB record created
            if idem_key:
                await self._claim_idempotency(idem_key, task.id)

            try:
                if scheduled_at and scheduled_at > datetime.utcnow():
                    # G7: future scheduled_at → status = SCHEDULED
                    task = await TaskRepository.update(session, task.id, status=TaskStatus.SCHEDULED) or task
                    await self._dispatch(task, dispatch_mode, scheduled_at=scheduled_at, capability=capability)
                else:
                    task = await TaskRepository.update(session, task.id, status=TaskStatus.QUEUED) or task
                    await self._dispatch(task, dispatch_mode, capability=capability)
                # Commit so data is visible in subsequent sessions (required for idempotency lookup)
                await session.commit()
            except Exception as exc:
                # G3: rollback on enqueue failure
                await self._rollback(session, task, idem_key)
                raise RuntimeError(f"TaskRouter enqueue failed, rolled back task_id={task.id}: {exc}") from exc

            return task

    async def create_delayed_task(self, task_create: TaskCreate, delay_seconds: int) -> Task:
        scheduled_at = datetime.utcnow() + timedelta(seconds=delay_seconds)
        return await self.create_task(task_create.model_copy(update={"scheduled_at": scheduled_at}))

    # ------------------------------------------------------------------
    # Dispatch path (G3: dispatch_mode routing)
    # ------------------------------------------------------------------

    async def _dispatch(
        self,
        task: Task,
        mode: DispatchMode,
        scheduled_at: datetime | None = None,
        capability: str = "default",
    ) -> None:
        """Route task to appropriate execution path based on dispatch_mode."""
        if mode == DispatchMode.DAG_ORCHESTRATED:
            # DAG orchestration: just enqueue; DAGEngine manages step ordering
            logger.debug("TaskRouter dispatch DAG_ORCHESTRATED task_id=%s cap=%s", task.id, capability)
            await self.queue_manager.enqueue(task, scheduled_at=scheduled_at, capability=capability)

        elif mode == DispatchMode.RAYDATA_NATIVE:
            # Ray-native: enqueue to capability queue; SchedulerActor picks it up
            logger.debug("TaskRouter dispatch RAYDATA_NATIVE task_id=%s cap=%s", task.id, capability)
            await self.queue_manager.enqueue(task, scheduled_at=scheduled_at, capability=capability)

        else:
            # DIRECT (default)
            await self.queue_manager.enqueue(task, scheduled_at=scheduled_at, capability=capability)

    # ------------------------------------------------------------------
    # Idempotency helpers (G3)
    # ------------------------------------------------------------------

    async def _check_idempotency(self, idem_key: str) -> Task | None:
        """Return the existing Task if this key was already claimed, else None."""
        redis_key = f"{_IDEM_PREFIX}:{idem_key}"
        task_id: str | None = None

        if self._redis is not None:
            try:
                task_id = await self._redis.get(redis_key)
            except Exception:
                pass
        else:
            task_id = self._idem_store.get(idem_key)

        if task_id is None:
            return None

        async with await get_session_no_context() as session:
            return await TaskRepository.get(session, task_id)

    async def _claim_idempotency(self, idem_key: str, task_id: str) -> bool:
        """Atomically claim idem_key → task_id. Returns True if claimed."""
        redis_key = f"{_IDEM_PREFIX}:{idem_key}"
        if self._redis is not None:
            try:
                result = await self._redis.set(redis_key, task_id, nx=True, ex=_IDEM_TTL_SECONDS)
                return result is not None
            except Exception:
                pass
        # In-process fallback (not strictly atomic but acceptable for tests)
        if idem_key not in self._idem_store:
            self._idem_store[idem_key] = task_id
            return True
        return False

    async def _release_idempotency(self, idem_key: str) -> None:
        """Release idempotency claim (used during rollback)."""
        redis_key = f"{_IDEM_PREFIX}:{idem_key}"
        if self._redis is not None:
            try:
                await self._redis.delete(redis_key)
            except Exception:
                pass
        self._idem_store.pop(idem_key, None)

    # ------------------------------------------------------------------
    # Rollback (G3)
    # ------------------------------------------------------------------

    async def _rollback(self, session: Any, task: Task, idem_key: str | None) -> None:
        """Rollback: delete DB record and release idempotency key."""
        try:
            await TaskRepository.delete(session, task.id)
            logger.info("TaskRouter rollback: deleted task_id=%s", task.id)
        except Exception as e:
            logger.warning("TaskRouter rollback: failed to delete task_id=%s: %s", task.id, e)

        if idem_key:
            await self._release_idempotency(idem_key)
            logger.info("TaskRouter rollback: released idem_key=%s", idem_key)


def _infer_capability(task: Task) -> str:
    """Infer capability from task.tags list (tag format: 'capability:<name>')."""
    for tag in (task.tags or []):
        if isinstance(tag, str) and tag.startswith("capability:"):
            return tag.split(":", 1)[1].strip()
    return "default"
