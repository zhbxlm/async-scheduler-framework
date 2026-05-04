"""TaskCreator — adapts task creation requests for QueueManager and CronScheduler.

Provides create_task method that:
1. Generates task_id if not provided via idempotency_key
2. Stores task metadata in Redis (task:{task_id})
3. Enqueues via QueueManager
4. Optionally persists to MySQL (if db_session_factory provided)

Used by CronScheduler to fire scheduled tasks.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

logger = logging.getLogger(__name__)


class TaskCreator:
    """Adapts task creation for QueueManager."""

    def __init__(
        self,
        redis_client: Any,
        queue_manager: Any,
        *,
        db_session_factory: Any | None = None,
    ) -> None:
        self._r = redis_client
        self._qm = queue_manager
        self._db = db_session_factory

    async def create_task(
        self,
        *,
        tenant_id: str = "",
        idempotency_key: str | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """Create and enqueue a task.
        
        Args:
            tenant_id: Tenant identifier
            idempotency_key: Optional idempotency key (used for dedup)
            **kwargs: Task parameters from template (must include task_type/capability)
        
        Returns:
            Dict with task_id, queue_position, pending_count, etc.
        
        Raises:
            ValueError if task_type/capability missing
        """
        # Extract capability (task_type)
        capability = kwargs.get("task_type", kwargs.get("capability", ""))
        if not capability:
            raise ValueError("task_type or capability must be provided in template")

        # Generate task_id
        if idempotency_key:
            # Use idempotency_key as deterministic task_id prefix
            task_id = f"cron-{idempotency_key}-{uuid.uuid4().hex[:16]}"
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

        # Store in Redis
        task_key = f"task:{task_id}"
        try:
            await self._r.set(task_key, json.dumps(task_record), ex=86400)  # 24h TTL
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

        # Optionally persist to MySQL
        if self._db and kwargs.get("persist_to_db", True):
            try:
                await self._persist_to_db(task_id, tenant_id, capability, kwargs)
            except Exception as exc:
                logger.warning("TaskCreator: DB persistence failed: %s", exc)

        return {
            "task_id": task_id,
            "accepted": result.get("accepted", False),
            "queue_position": result.get("queue_position", -1),
            "pending_count": result.get("pending_count", 0),
        }

    async def _persist_to_db(
        self,
        task_id: str,
        tenant_id: str,
        task_type: str,
        template: dict[str, Any],
    ) -> None:
        """Persist task record to MySQL (if configured)."""
        if not self._db:
            return

        from src.models.task import TaskRecord, TaskStatus, TaskPriority, TaskDispatchMode
        
        async with self._db() as session:
            record = TaskRecord(
                task_id=task_id,
                tenant_id=tenant_id,
                task_type=task_type,
                status=TaskStatus.QUEUED,
                priority=TaskPriority(template.get("priority", "normal")),
                dispatch_mode=TaskDispatchMode(template.get("dispatch_mode", "dag_orchestrated")),
                input_data=json.dumps(template.get("input_data", {})),
                metadata_json=json.dumps(template.get("metadata", {})),
                callback_url=template.get("callback_url"),
                idempotency_key=template.get("idempotency_key"),
                timeout_seconds=template.get("timeout_seconds", 3600),
                max_retries=template.get("max_retries", 3),
                scheduled_at=template.get("scheduled_at"),
                cron_expr=template.get("cron_expr"),
            )
            session.add(record)
            await session.commit()
            logger.debug("TaskCreator: persisted task %s to DB", task_id)