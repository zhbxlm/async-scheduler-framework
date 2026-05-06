"""TaskCreator — adapts task creation requests for QueueManager.

Provides create_task method that:
1. MySQL-first atomic write (durable)
2. Redis sync (fast path, best-effort)
3. Compensation service for Redis failures
4. Enqueues via QueueManager

Used by CronScheduler to fire scheduled tasks.
"""
from __future__ import annotations

import json  # stdlib — MySQL TEXT column persistence (orjson returns bytes)
import logging
import time
import uuid
from typing import Any

import orjson

from src.common.error_handling import log_errors, BusinessError
from src.common.transaction import AtomicWriteCoordinator

logger = logging.getLogger(__name__)


class TaskCreator:
    """Adapts task creation for QueueManager."""

    def __init__(
        self,
        redis_client: Any,
        queue_manager: Any,
        *,
        db_session_factory: Any | None = None,
        coordinator: AtomicWriteCoordinator | None = None,
    ) -> None:
        self._r = redis_client
        self._qm = queue_manager
        self._db = db_session_factory
        self._coordinator = coordinator
        
        # Create coordinator if not provided
        if self._db and not self._coordinator:
            self._coordinator = AtomicWriteCoordinator(
                mysql_session_factory=self._db,
                redis_client=self._r,
                enable_logging=True,
            )

    @log_errors(log_level="ERROR", raise_exception=True)
    async def create_task(
        self,
        *,
        tenant_id: str = "",
        idempotency_key: str | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """Create and enqueue a task.
        
        Idempotency: if idempotency_key is provided, checks DB for an
        existing task with the same (tenant_id, idempotency_key) pair.
        Returns the existing task if found — no duplicate is created.
        
        Args:
            tenant_id: Tenant identifier
            idempotency_key: Optional idempotency key (used for dedup)
            **kwargs: Task parameters from template (must include task_type/capability)
        
        Returns:
            Dict with task_id, queue_position, pending_count, etc.
            "idempotent_reused": True if an existing task was returned.
        
        Raises:
            ValueError if task_type/capability missing
        """
        # Extract capability (task_type)
        capability = kwargs.get("task_type", kwargs.get("capability", ""))
        if not capability:
            raise ValueError("task_type or capability must be provided in template")

        # ── Idempotency check (DB lookup) ────────────────────────
        if idempotency_key and self._db:
            existing = await self._find_existing(tenant_id, idempotency_key)
            if existing:
                # Return the existing task_id for true idempotency
                return {
                    "task_id": existing.task_id,
                    "accepted": True,
                    "queue_position": -1,
                    "pending_count": 0,
                    "idempotent_reused": True,
                }

        # Generate task_id
        if idempotency_key:
            # Generate deterministic ID for idempotent tasks
            safe_key = idempotency_key.replace(":", "-")[:60]
            # Use hash of idempotency_key for deterministic ID
            import hashlib
            key_hash = hashlib.sha256(idempotency_key.encode()).hexdigest()[:16]
            task_id = f"cron-{safe_key}-{key_hash}"
        else:
            task_id = str(uuid.uuid4())

        # Priority from kwargs or default
        priority_str = kwargs.get("priority", "normal")
        priority_map = {
            "very_high": 1,
            "high": 2,
            "normal": 3,
            "low": 4,
            "tide": 5,
        }
        priority = priority_map.get(priority_str.lower(), 3)

        # Execute after (delay)
        execute_after_ms = 0
        delay_seconds = kwargs.get("delay_seconds")
        if delay_seconds and delay_seconds > 0:
            execute_after_ms = int(time.time() * 1000) + (delay_seconds * 1000)

        # Build Redis task record
        task_record = {
            "task_id": task_id,
            "tenant_id": tenant_id,
            "status": "queued",
            "created_at_ts": time.time(),
            "capability": capability,
            "priority_rank": priority,
            **kwargs,
        }

        # ── MySQL-first atomic write (if DB configured) ──────────
        if self._db and kwargs.get("persist_to_db", True) and self._coordinator:
            try:
                from src.models.task import TaskRecord, TaskStatus, TaskPriority
                import hashlib
                
                # MySQL write function
                async def mysql_write():
                    async with self._db() as session:
                        record = TaskRecord(
                            task_id=task_id,
                            tenant_id=tenant_id,
                            task_type=capability,
                            status=TaskStatus.QUEUED,
                            priority=TaskPriority(kwargs.get("priority", "normal")),
                            input_data=json.dumps(kwargs.get("input_data", {})),
                            metadata_json=json.dumps(kwargs.get("metadata", {})),
                            callback_url=kwargs.get("callback_url"),
                            idempotency_key=kwargs.get("idempotency_key"),
                            timeout_seconds=kwargs.get("timeout_seconds", 3600),
                            max_retries=kwargs.get("max_retries", 3),
                            scheduled_at=kwargs.get("scheduled_at"),
                            cron_expr=kwargs.get("cron_expr"),
                        )
                        session.add(record)
                        await session.commit()
                
                # Redis write function
                async def redis_write():
                    task_key = f"task:{task_id}"
                    await self._r.set(task_key, orjson.dumps(task_record), ex=86400)
                
                # Execute atomic write
                tx = await self._coordinator.write_atomic(
                    task_id=task_id,
                    operation="create_task",
                    mysql_write_fn=mysql_write,
                    redis_write_fn=redis_write,
                    payload={"tenant_id": tenant_id, "capability": capability},
                )
                
                if tx.status != "committed":
                    # Redis write failed but MySQL succeeded
                    # Continue anyway - compensation service will fix it
                    logger.warning(
                        "TaskCreator: Redis sync failed for %s (MySQL OK), compensation enqueued",
                        task_id
                    )
                
            except Exception as exc:
                # MySQL or coordinator failed - abort
                logger.error("TaskCreator: atomic write failed for %s: %s", task_id, exc)
                raise
                
        else:
            # Fallback: old Redis-only mode
            task_key = f"task:{task_id}"
            try:
                await self._r.set(task_key, orjson.dumps(task_record), ex=86400)  # 24h TTL
            except Exception as exc:
                logger.warning("TaskCreator: failed to store task in Redis: %s", exc)
                # Continue anyway, queue may still work

        # Enqueue
        result = await self._qm.enqueue(
            capability=capability,
            task_id=task_id,
            priority=priority,
            execute_after_ms=execute_after_ms,
        )

        return {
            "task_id": task_id,
            "accepted": result.get("accepted", False),
            "queue_position": result.get("queue_position", -1),
            "pending_count": result.get("pending_count", 0),
        }

    async def _find_existing(self, tenant_id: str, idempotency_key: str) -> Any:
        """Look up an existing task by tenant + idempotency key."""
        if not self._db:
            return None
        from sqlalchemy import select
        from src.models.task import TaskRecord
        async with self._db() as session:
            result = await session.execute(
                select(TaskRecord).where(
                    TaskRecord.tenant_id == tenant_id,
                    TaskRecord.idempotency_key == idempotency_key,
                )
            )
            return result.scalar_one_or_none()

