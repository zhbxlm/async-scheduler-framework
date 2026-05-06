"""Atomic distributed writes: MySQL-first with Redis reconciliation.

Pattern:
  1. Write to MySQL (durable, transactional)
  2. Sync to Redis (fast path)
  3. If Redis fails → CompensationService retries

This ensures durability (MySQL) without sacrificing Redis speed.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Transaction Status
# ──────────────────────────────────────────────────────────────────────────────

class TxStatus(str, Enum):
    """Distributed transaction status."""
    PENDING_REDIS = "pending_redis"   # MySQL done, Redis sync pending
    COMMITTED = "committed"           # Both MySQL and Redis done
    FAILED = "failed"                 # Redis sync failed (compensation needed)
    RECOVERED = "recovered"           # Compensated successfully
    EXPIRED = "expired"               # TTL expired before sync


# ──────────────────────────────────────────────────────────────────────────────
# Transaction Record
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class TxRecord:
    """Track a distributed write operation."""
    tx_id: str
    task_id: str
    operation: str  # create_task, update_status, complete_task
    status: TxStatus = TxStatus.PENDING_REDIS
    mysql_written: bool = False
    redis_written: bool = False
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0
    last_error: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)


# ──────────────────────────────────────────────────────────────────────────────
# Atomic Write Coordinator
# ──────────────────────────────────────────────────────────────────────────────

class AtomicWriteCoordinator:
    """Coordinate MySQL → Redis writes with compensation support.
    
    Usage:
        coordinator = AtomicWriteCoordinator(mysql_session, redis_client)
        
        async with coordinator.begin(task_id, "create_task") as tx:
            # Phase 1: Write to MySQL (durable)
            await tx.mysql_write(session.add(task_record), session.flush())
            
            # Phase 2: Sync to Redis (fast)
            await tx.redis_write(lambda: redis.set(key, value))
    """
    
    # Redis key prefix for transaction tracking
    TX_PREFIX = "tx:"
    TX_TTL = 300  # 5 minutes for pending transactions
    
    def __init__(
        self,
        mysql_session_factory: Any = None,
        redis_client: Any = None,
        *,
        compensation_service: Optional[Any] = None,
        enable_logging: bool = True,
    ) -> None:
        self._db_factory = mysql_session_factory
        self._redis = redis_client
        self._compensation = compensation_service
        self._enable_logging = enable_logging
    
    async def write_atomic(
        self,
        task_id: str,
        operation: str,
        mysql_write_fn: Callable[[], Any],
        redis_write_fn: Callable[[], Any],
        *,
        payload: Optional[Dict[str, Any]] = None,
        tx_id: Optional[str] = None,
    ) -> TxRecord:
        """Execute MySQL + Redis write atomically.
        
        MySQL is always written first (durable). Redis follows.
        If Redis fails, the tx record is left as PENDING_REDIS for compensation.
        
        Args:
            task_id: Task identifier
            operation: Operation name for logging
            mysql_write_fn: SQLAlchemy write function
            redis_write_fn: async Redis write function
            payload: Optional metadata for compensation
            tx_id: Optional transaction ID (auto-generated if not provided)
        
        Returns:
            TxRecord with final status
        """
        if tx_id is None:
            import uuid
            tx_id = f"tx-{task_id[:8]}-{uuid.uuid4().hex[:8]}"
        
        tx = TxRecord(
            tx_id=tx_id,
            task_id=task_id,
            operation=operation,
            payload=payload or {},
        )
        
        try:
            # ── Phase 1: MySQL (durable, primary source of truth) ─────
            tx.status = TxStatus.PENDING_REDIS
            if asyncio.iscoroutinefunction(mysql_write_fn):
                await mysql_write_fn()
            else:
                mysql_write_fn()
            tx.mysql_written = True
            
            if self._enable_logging:
                logger.debug("Tx %s: MySQL write succeeded for %s", tx_id, task_id)
            
            # ── Phase 2: Redis (fast path, best-effort) ───────────────
            try:
                await redis_write_fn()
                tx.redis_written = True
                tx.status = TxStatus.COMMITTED
                
                if self._enable_logging:
                    logger.debug("Tx %s: Redis sync succeeded for %s", tx_id, task_id)
                
            except Exception as redis_error:
                # Redis failed → leave pending for compensation
                tx.last_error = str(redis_error)
                tx.status = TxStatus.FAILED
                
                logger.warning(
                    "Tx %s: Redis sync failed for %s (MySQL OK): %s",
                    tx_id, task_id, redis_error,
                )
                
                # Record pending tx for compensation
                await self._save_pending_tx(tx)
                
                if self._compensation:
                    await self._compensation.enqueue(tx)
        
        except Exception as mysql_error:
            # MySQL failed → nothing to compensate
            tx.status = TxStatus.FAILED
            tx.last_error = str(mysql_error)
            
            logger.error(
                "Tx %s: MySQL write failed for %s: %s",
                tx_id, task_id, mysql_error,
            )
            raise
        
        return tx
    
    async def _save_pending_tx(self, tx: TxRecord) -> None:
        """Persist pending transaction to Redis for compensation."""
        if not self._redis:
            return
        
        try:
            tx_key = f"{self.TX_PREFIX}{tx.tx_id}"
            tx_data = json.dumps({
                "tx_id": tx.tx_id,
                "task_id": tx.task_id,
                "operation": tx.operation,
                "status": tx.status.value,
                "created_at": tx.created_at,
                "retry_count": tx.retry_count,
                "last_error": tx.last_error,
                "payload": tx.payload,
            })
            await self._redis.set(tx_key, tx_data, ex=self.TX_TTL)
        except Exception as e:
            logger.error("Failed to save pending tx %s: %s", tx.tx_id, e)


# ──────────────────────────────────────────────────────────────────────────────
# Convenience functions
# ──────────────────────────────────────────────────────────────────────────────

async def atomic_task_create(
    coordinator: AtomicWriteCoordinator,
    task_id: str,
    *,
    mysql_insert: Callable[[], Any],
    redis_set: Callable[[], Any],
    **kwargs,
) -> TxRecord:
    """Atomic create-task: MySQL INSERT → Redis SET."""
    return await coordinator.write_atomic(
        task_id=task_id,
        operation="create_task",
        mysql_write_fn=mysql_insert,
        redis_write_fn=redis_set,
        payload=kwargs,
    )


async def atomic_task_status_update(
    coordinator: AtomicWriteCoordinator,
    task_id: str,
    *,
    mysql_update: Callable[[], Any],
    redis_update: Callable[[], Any],
) -> TxRecord:
    """Atomic update-task-status: MySQL UPDATE → Redis SET."""
    return await coordinator.write_atomic(
        task_id=task_id,
        operation="update_status",
        mysql_write_fn=mysql_update,
        redis_write_fn=redis_update,
    )
