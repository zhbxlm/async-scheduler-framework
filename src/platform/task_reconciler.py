"""TaskReconciler — aligned with docs/deepwiki-reference/任务执行.md

Reconciliation direction is now MySQL-first:
  Phase 0 (durable rebuild): Rebuild Redis task cache from MySQL durable truth
  Phase 1 (double-write legacy): Scan Redis terminal tasks → persist missing to MySQL
  Phase 2 (stuck recovery): Detect expired execution locks → FAILED or requeue
  Phase 3 (lost callback): Compensate missing callbacks for terminal tasks

Redis `task:*` is treated as cache/index state, not the durable source of truth.
Uses a shared SCAN cursor across Redis-oriented phases to amortize scan cost.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any

import json

from src.common.error_handling import log_errors
from src.common.ttl_constants import CALLBACK_RETRY_PAYLOAD_TTL

logger = logging.getLogger(__name__)

_TASK_KEY_PATTERN = "task:*"
_LOCK_KEY_TEMPLATE = "task_lock:{task_id}"
_CALLBACK_DONE_KEY = "callback:done:{task_id}"
_CALLBACK_RETRY_KEY = "callback:retry:pending"
_REQUEUE_DEDUP_KEY = "requeue_dedup:{task_id}"
_REQUEUE_DEDUP_TTL = 300
_STUCK_MAX_PER_TICK = 20
_STUCK_TASK_MAX_AGE_SECONDS = 300

_LUA_RENEW_LEADER = """
local current = redis.call("GET", KEYS[1])
if current and current == ARGV[1] then
    return redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]))
end
return 0
"""
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
        compensation_service: Any | None = None,
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
        self._compensation = compensation_service
        self._lua_renew_leader = redis_client.register_script(_LUA_RENEW_LEADER) if redis_client else None
        self._running = False
        self._scan_cursor: int = 0
        self._loop_task: asyncio.Task | None = None

    @log_errors(log_level="INFO", raise_exception=False)
    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._loop_task = asyncio.create_task(self._loop())
        logger.info("TaskReconciler started interval=%.1fs", self._interval)

    async def stop(self) -> None:
        self._running = False
        if self._loop_task:
            try:
                await asyncio.wait_for(self._loop_task, timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("TaskReconciler: stop timeout")
            except Exception as exc:
                logger.warning("TaskReconciler: stop error: %s", exc)
            self._loop_task = None

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
        renewed = await self._lua_renew_leader(
            keys=[_RECONCILE_LEADER_KEY],
            args=[self._instance_id, str(self._leader_ttl)]
        )
        if renewed == 1:
            return True
        return False

    @log_errors(log_level="WARNING", raise_exception=False)
    async def _loop(self) -> None:
        while self._running:
            if await self._try_become_leader():
                # Phase 0: MySQL is authoritative; rebuild missing Redis cache/index state first.
                await self._rebuild_redis_from_mysql()
                task_batch = await self._scan_task_batch()
                if task_batch:
                    await asyncio.gather(
                        self._phase1_double_write(task_batch),
                        self._phase2_stuck_recovery(task_batch),
                        self._phase3_lost_callback(task_batch),
                        return_exceptions=True,
                    )
            await asyncio.sleep(self._interval)

    async def _rebuild_redis_from_mysql(self) -> None:
        """Rebuild missing Redis task cache from MySQL durable state.

        MySQL is the source of truth. Redis `task:*` is a repairable cache layer.
        This method only fills missing Redis cache entries and intentionally avoids
        mutating queue/running indexes until a later refactor step.
        """
        if self._db is None or self._r is None:
            return
        try:
            async with self._db() as session:
                from sqlalchemy import select
                from src.models.task import TaskRecord

                result = await session.execute(select(TaskRecord))
                rows = result.scalars().all()

                if not rows:
                    return

                for row in rows:
                    task_id = getattr(row, "task_id", None)
                    if not task_id:
                        continue

                    def _enum_value(v):
                        return getattr(v, "value", v)

                    def _json_load_or_default(v, default):
                        if not v:
                            return default
                        if isinstance(v, (dict, list)):
                            return v
                        try:
                            return json.loads(v)
                        except Exception:
                            return default

                    status = _enum_value(getattr(row, "status", "pending"))
                    capability = getattr(row, "task_type", "") or ""
                    payload = {
                        "task_id": task_id,
                        "tenant_id": getattr(row, "tenant_id", "") or "",
                        "status": status,
                        "capability": capability,
                        "priority": _enum_value(getattr(row, "priority", "normal")),
                        "input_data": _json_load_or_default(getattr(row, "input_data", None), {}),
                        "output": _json_load_or_default(getattr(row, "output_data", None), {}),
                        "error_message": getattr(row, "error_message", None),
                        "metadata": _json_load_or_default(getattr(row, "metadata_json", None), {}),
                        "callback_url": getattr(row, "callback_url", None) or "",
                        "idempotency_key": getattr(row, "idempotency_key", None),
                        "timeout_seconds": getattr(row, "timeout_seconds", 3600) or 3600,
                        "max_retries": getattr(row, "max_retries", 3) or 3,
                        "attempt": getattr(row, "attempt", 0) or 0,
                        "scheduled_at": str(getattr(row, "scheduled_at", None)) if getattr(row, "scheduled_at", None) else None,
                        "cron_expr": getattr(row, "cron_expr", None),
                        "created_at": str(getattr(row, "created_at", None)) if getattr(row, "created_at", None) else None,
                        "updated_at": str(getattr(row, "updated_at", None)) if getattr(row, "updated_at", None) else None,
                    }

                    # Rebuild cache if missing.
                    task_key = f"task:{task_id}"
                    existing = await self._r.get(task_key)
                    if not existing:
                        await self._r.set(task_key, json.dumps(payload), ex=86400)

                    # Conservative index rebuild: only restore pending queue membership
                    # for queued/scheduled tasks when task is absent from both pending/running.
                    if capability and status in ("queued", "scheduled"):
                        pending_key = f"{{queue:{capability}}}:pending"
                        running_key = f"{{queue:{capability}}}:running"
                        in_pending = await self._r.zscore(pending_key, task_id)
                        in_running = await self._r.zscore(running_key, task_id)
                        if in_pending is None and in_running is None:
                            await self._r.zadd(pending_key, {task_id: time.time()})
        except Exception as exc:
            logger.warning("TaskReconciler: mysql->redis rebuild error: %s", exc)

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
                data = json.loads(raw)
                if isinstance(data, dict):
                    tasks.append(data)
            except (json.JSONDecodeError, TypeError):
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
                            output_data=json.dumps(task.get("output")) if task.get("output") else None,
                        )
                        session.add(record)
                    except Exception as exc:
                        logger.warning(
                            "TaskReconciler: phase1 insert failed task_id=%s: %s",
                            task.get("task_id"), exc,
                        )
                await session.commit()
                logger.info("TaskReconciler: phase1 compensated %d tasks", len(missing))

                # Notify CompensationService so it can also repair Redis-side
                if self._compensation:
                    for task in missing:
                        from src.common.transaction import TxRecord, TxStatus
                        tx = TxRecord(
                            tx_id=f"reconciler-{task.get('task_id', 'unknown')[:8]}",
                            task_id=task.get("task_id", ""),
                            operation="create_task",
                            status=TxStatus.FAILED,
                            mysql_written=True,
                            redis_written=False,
                            last_error="detected by reconciler phase1",
                        )
                        try:
                            await self._compensation.enqueue(tx)
                        except Exception as comp_err:
                            logger.warning(
                                "TaskReconciler: compensation enqueue failed: %s", comp_err
                            )
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
        
        # Collect tasks for batch processing
        requeue_tasks = []  # Tasks to requeue with incremented attempt
        fail_tasks = []     # Tasks to mark as failed (max retries exceeded)
        
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
                # Collect for batch requeue
                requeue_tasks.append({
                    "task_id": tid,
                    "attempt": attempt,
                    "capability": task.get("capability", task.get("task_type", "")),
                    "priority_rank": task.get("priority_rank", 3),
                    "max_retries": max_retries,
                })
            else:
                # Mark as failed (max retries exceeded)
                fail_tasks.append(tid)
            
            stuck_count += 1
        
        # ── Batch Database Updates ──
        if self._db is not None and requeue_tasks:
            try:
                from sqlalchemy import update
                from src.models.task import TaskRecord
                
                task_ids = [t["task_id"] for t in requeue_tasks]
                
                async with self._db() as session:
                    # Batch update attempt counter
                    stmt = (
                        update(TaskRecord)
                        .where(TaskRecord.task_id.in_(task_ids))
                        .values(attempt=TaskRecord.attempt + 1)
                    )
                    await session.execute(stmt)
                    await session.commit()
                    
                    logger.info(
                        "TaskReconciler: batch updated %d tasks (attempt+1)",
                        len(task_ids)
                    )
                    
            except Exception as exc:
                logger.warning(
                    "TaskReconciler: batch update failed: %s", exc
                )
        
        # ── Batch Requeue Operations ──
        current_time_ms = int(time.time() * 1000)
        for task_info in requeue_tasks:
            tid = task_info["task_id"]
            attempt = task_info["attempt"]
            capability = task_info["capability"]
            priority_rank = task_info["priority_rank"]
            
            delay_ms = min(600_000, 10_000 * (2 ** attempt))
            
            if capability:
                await self._qm.enqueue(
                    capability,
                    tid,
                    priority=priority_rank,
                    execute_after_ms=current_time_ms + delay_ms,
                )
                logger.info(
                    "TaskReconciler: phase2 requeued task_id=%s attempt=%d", 
                    tid, attempt
                )
        
        # ── Batch Fail Operations (pipelined GETs) ──
        if fail_tasks:
            fail_keys = [f"task:{tid}" for tid in fail_tasks]
            
            # Pipeline all GETs
            pipe = self._r.pipeline()
            for key in fail_keys:
                pipe.get(key)
            raw_results = await pipe.execute()
            
            # Write-back with another pipeline
            write_pipe = self._r.pipeline()
            for tid, key, raw in zip(fail_tasks, fail_keys, raw_results):
                if raw:
                    try:
                        data = json.loads(raw)
                        data["status"] = "failed"
                        data["error"] = "reconciler: stuck task, lock expired"
                        write_pipe.set(key, json.dumps(data))
                        logger.info(
                            "TaskReconciler: phase2 marked FAILED task_id=%s", tid
                        )
                    except Exception as exc:
                        logger.warning(
                            "TaskReconciler: phase2 update error task_id=%s: %s",
                            tid, exc
                        )
            await write_pipe.execute()

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

            # Use task_id as the sorted-set member for O(log N) dedup lookup.
            # Full payload is stored in a companion hash key.
            already_queued = await self._r.zscore(_CALLBACK_RETRY_KEY, tid) is not None
            if already_queued:
                continue

            retry_payload_key = f"callback:retry:payload:{tid}"
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
            # Atomic pipeline: add member + store payload together
            pipe = self._r.pipeline()
            pipe.zadd(_CALLBACK_RETRY_KEY, {tid: next_retry_ts})
            pipe.set(retry_payload_key, retry_event, ex=CALLBACK_RETRY_PAYLOAD_TTL)
            await pipe.execute()
            compensated += 1
            logger.info("TaskReconciler: phase3 enqueued callback task_id=%s", tid)

        if compensated:
            logger.info("TaskReconciler: phase3 compensated %d callbacks", compensated)
