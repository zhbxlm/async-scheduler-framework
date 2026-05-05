"""TaskReconciler — aligned with docs/deepwiki-reference/任务执行.md

3-phase consistency repair loop:
  Phase 1 (double-write): Scan Redis terminal tasks → persist missing to MySQL
  Phase 2 (stuck recovery): Detect expired execution locks → FAILED or requeue
  Phase 3 (lost callback): Compensate missing callbacks for terminal tasks

Uses a shared SCAN cursor across all three phases to amortize Redis scan cost.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any

import orjson

from src.common.error_handling import log_errors, ExternalServiceError

logger = logging.getLogger(__name__)

_TASK_KEY_PATTERN = "task:*"
_LOCK_KEY_TEMPLATE = "task_lock:{task_id}"
_CALLBACK_DONE_KEY = "callback:done:{task_id}"
_CALLBACK_RETRY_KEY = "callback:retry:pending"
_REQUEUE_DEDUP_KEY = "requeue_dedup:{task_id}"
_REQUEUE_DEDUP_TTL = 300
_STUCK_MAX_PER_TICK = 20
_STUCK_TASK_MAX_AGE_SECONDS = 300
_RECONCILE_LEADER_KEY = "reconcile_leader"
_LEADER_TTL = 90  # seconds


class TaskReconciler:
    """Three-phase consistency repair for Redis ↔ MySQL task state."""

    def __init__(
        self,
        redis_client: Any | None = None,
        db_session_factory: Any | None = None,
        queue_manager: Any | None = None,
        *,
        interval_seconds: float = 15.0,
        stuck_max_per_tick: int = _STUCK_MAX_PER_TICK,
        stuck_task_max_age_seconds: float = _STUCK_TASK_MAX_AGE_SECONDS,
        batch_size: int = 100,
        instance_id: str | None = None,
        leader_ttl: int = _LEADER_TTL,
    ) -> None:
        self._r = redis_client
        self._db = db_session_factory
        self._qm = queue_manager
        self._interval = interval_seconds
        self._stuck_max_per_tick = stuck_max_per_tick
        self._stuck_max_age = stuck_task_max_age_seconds
        self._batch_size = batch_size
        self._instance_id = instance_id or str(uuid.uuid4())
        self._leader_ttl = leader_ttl
        self._running = False
        self._scan_cursor: int = 0

    @log_errors(log_level="INFO", raise_exception=False)
    async def start(self) -> None:
        self._running = True
        logger.info("TaskReconciler started interval=%.1fs", self._interval)
        asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _try_become_leader(self) -> bool:
        """Redis SET NX EX for leader election."""
        if self._r is None:
            return True  # no redis → single instance mode
        ok = await self._r.set(_RECONCILE_LEADER_KEY, self._instance_id, nx=True, ex=self._leader_ttl)
        if ok:
            return True
        current = await self._r.get(_RECONCILE_LEADER_KEY)
        current_id = current.decode() if isinstance(current, bytes) else current
        if current_id == self._instance_id:
            await self._r.expire(_RECONCILE_LEADER_KEY, self._leader_ttl)
            return True
        return False

    @log_errors(log_level="WARNING", raise_exception=False)
    async def _loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._interval)
            if not await self._try_become_leader():
                continue  # not leader, skip tick
            task_batch = await self._scan_task_batch()
            if task_batch:
                await asyncio.gather(
                    self._phase1_double_write(task_batch),
                    self._phase2_stuck_recovery(task_batch),
                    self._phase3_lost_callback(task_batch),
                    return_exceptions=True,
                )

    async def _scan_task_batch(self) -> list[dict]:
        """SCAN Redis for task:* keys and return parsed task dicts."""
        if self._r is None:
            return []
        try:
            cursor, keys = await self._r.scan(
                self._scan_cursor, match=_TASK_KEY_PATTERN, count=self._batch_size
            )
            self._scan_cursor = cursor  # advance cursor (0 = full cycle done)
        except Exception as exc:
            logger.warning("TaskReconciler: scan error: %s", exc)
            return []

        if not keys:
            return []

        pipe = self._r.pipeline()
        for k in keys:
            pipe.get(k)
        raws = await pipe.execute()

        tasks = []
        for raw in raws:
            if not raw:
                continue
            try:
                data = orjson.loads(raw)
                if isinstance(data, dict):
                    tasks.append(data)
            except (orjson.JSONDecodeError, TypeError):
                pass
        return tasks

    # ------------------------------------------------------------------
    # Phase 1: Double-write reconciliation
    # ------------------------------------------------------------------

    async def _phase1_double_write(self, task_batch: list[dict]) -> None:
        """Ensure terminal tasks in Redis are persisted to MySQL."""
        if self._db is None:
            return

        terminal_statuses = {"completed", "failed", "cancelled"}
        terminal_tasks = [
            t for t in task_batch
            if t.get("status", "").lower() in terminal_statuses
        ]
        if not terminal_tasks:
            return

        task_ids = [t["task_id"] for t in terminal_tasks if t.get("task_id")]
        if not task_ids:
            return

        try:
            async with self._db() as session:
                from sqlalchemy import select
                from src.models.task import TaskRecord

                result = await session.execute(
                    select(TaskRecord.task_id).where(TaskRecord.task_id.in_(task_ids))
                )
                persisted_ids = {row[0] for row in result.fetchall()}

                missing = [t for t in terminal_tasks if t["task_id"] not in persisted_ids]
                if not missing:
                    return

                for task in missing:
                    try:
                        from src.models.task import TaskRecord
                        record = TaskRecord(
                            task_id=task.get("task_id", ""),
                            tenant_id=task.get("tenant_id", ""),
                            status=task.get("status", "unknown"),
                            output=orjson.dumps(task.get("output")).decode() if task.get("output") else None,
                        )
                        session.add(record)
                    except Exception as exc:
                        logger.warning(
                            "TaskReconciler: phase1 insert failed task_id=%s: %s",
                            task.get("task_id"), exc,
                        )
                await session.commit()
                logger.info("TaskReconciler: phase1 compensated %d tasks", len(missing))
        except Exception as exc:
            logger.error("TaskReconciler: phase1 error: %s", exc)

    # ------------------------------------------------------------------
    # Phase 2: Stuck task recovery
    # ------------------------------------------------------------------

    async def _phase2_stuck_recovery(self, task_batch: list[dict]) -> None:
        """Detect tasks stuck in RUNNING with expired execution locks."""
        if self._r is None:
            return

        running_tasks = [
            t for t in task_batch
            if t.get("status", "").lower() in ("running", "queued")
        ]
        if not running_tasks:
            return

        now = time.time()
        stuck_count = 0
        # Batch EXISTS check for execution locks
        pipe = self._r.pipeline()
        for t in running_tasks:
            tid = t.get("task_id", "")
            pipe.exists(_LOCK_KEY_TEMPLATE.format(task_id=tid))
        lock_results = await pipe.execute()

        for task, lock_exists in zip(running_tasks, lock_results):
            if stuck_count >= self._stuck_max_per_tick:
                break
            tid = task.get("task_id", "")
            if not tid:
                continue
            if lock_exists:
                continue  # lock alive → not stuck

            # Check task age
            created_ts = task.get("created_at_ts", 0)
            if created_ts and (now - float(created_ts)) < self._stuck_max_age:
                continue  # too young to be stuck

            # Dedup check
            dedup_key = _REQUEUE_DEDUP_KEY.format(task_id=tid)
            set_result = await self._r.set(dedup_key, "1", nx=True, ex=_REQUEUE_DEDUP_TTL)
            if not set_result:
                continue  # another reconciler already handling this

            max_retries = task.get("max_retries", 0)
            attempt = task.get("attempt", 0)

            if self._qm and attempt < max_retries:
                # Requeue with exponential backoff delay
                # Increment attempt counter in database atomically
                if self._db is not None:
                    try:
                        from sqlalchemy import update
                        from src.models.task import TaskRecord
                        async with self._db() as db_session:
                            stmt = (
                                update(TaskRecord)
                                .where(TaskRecord.task_id == tid)
                                .values(attempt=TaskRecord.attempt + 1)
                            )
                            await db_session.execute(stmt)
                            await db_session.commit()
                    except Exception as exc:
                        logger.warning(
                            "TaskReconciler: phase2 increment attempt failed task_id=%s: %s",
                            tid, exc,
                        )
                delay_ms = min(600_000, 10_000 * (2 ** attempt))
                capability = task.get("capability", task.get("task_type", ""))
                if capability:
                    await self._qm.enqueue(
                        capability,
                        tid,
                        priority=task.get("priority_rank", 3),
                        execute_after_ms=int(time.time() * 1000) + delay_ms,
                    )
                    logger.info(
                        "TaskReconciler: phase2 requeued task_id=%s attempt=%d", tid, attempt
                    )
            else:
                # Mark FAILED in Redis
                task_key = f"task:{tid}"
                raw = await self._r.get(task_key)
                if raw:
                    try:
                        data = orjson.loads(raw)
                        data["status"] = "failed"
                        data["error"] = "reconciler: stuck task, lock expired"
                        await self._r.set(task_key, orjson.dumps(data))
                        logger.info(
                            "TaskReconciler: phase2 marked FAILED task_id=%s", tid
                        )
                    except Exception as exc:
                        logger.warning("TaskReconciler: phase2 update error task_id=%s: %s", tid, exc)

            stuck_count += 1

    # ------------------------------------------------------------------
    # Phase 3: Lost callback recovery
    # ------------------------------------------------------------------

    async def _phase3_lost_callback(self, task_batch: list[dict]) -> None:
        """Compensate unsent callbacks for terminal tasks."""
        if self._r is None:
            return

        terminal_tasks = [
            t for t in task_batch
            if t.get("status", "").lower() in ("completed", "failed")
            and t.get("callback_url", "")
        ]
        if not terminal_tasks:
            return

        # Batch check callback done markers
        pipe = self._r.pipeline()
        for t in terminal_tasks:
            tid = t.get("task_id", "")
            pipe.exists(_CALLBACK_DONE_KEY.format(task_id=tid))
        done_results = await pipe.execute()

        compensated = 0
        for task, is_done in zip(terminal_tasks, done_results):
            if is_done:
                continue

            tid = task.get("task_id", "")
            callback_url = task.get("callback_url", "")
            if not callback_url:
                continue

            # Check if already in retry queue to avoid duplicates
            retry_members = await self._r.zrangebyscore(_CALLBACK_RETRY_KEY, "-inf", "+inf")
            already_queued = any(tid in (m.decode() if isinstance(m, bytes) else m)
                                 for m in retry_members)
            if already_queued:
                continue

            # Enqueue to durable callback retry queue
            retry_event = json.dumps({
                "task_id": tid,
                "callback_url": callback_url,
                "payload": {
                    "task_id": tid,
                    "status": task.get("status"),
                    "result": task.get("output"),
                },
                "attempt": 1,
            })
            next_retry_ts = time.time() + 10  # retry in 10s
            await self._r.zadd(_CALLBACK_RETRY_KEY, {retry_event: next_retry_ts})
            compensated += 1
            logger.info("TaskReconciler: phase3 enqueued callback task_id=%s", tid)

        if compensated:
            logger.info("TaskReconciler: phase3 compensated %d callbacks", compensated)
