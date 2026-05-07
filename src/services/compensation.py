"""Compensation service for Redis ↔ MySQL consistency.

Background service that:
1. Scans MySQL for tasks missing in Redis
2. Scans Redis for pending transactions
3. Retries failed Redis writes
4. Logs and alerts on persistent failures
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional

from sqlalchemy import select

from src.models.task import TaskRecord, TaskStatus
from src.common.transaction import TxStatus, TxRecord

logger = logging.getLogger(__name__)


class CompensationService:
    """Background service that repairs Redis ↔ MySQL inconsistencies."""
    
    def __init__(
        self,
        mysql_session_factory: Any,
        redis_client: Any,
        *,
        scan_interval_seconds: int = 60,
        batch_size: int = 100,
        max_retries: int = 3,
    ) -> None:
        self._db_factory = mysql_session_factory
        self._redis = redis_client
        self._scan_interval = scan_interval_seconds
        self._batch_size = batch_size
        self._max_retries = max_retries
        self._running = False
        self._task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:
        """Start the compensation service as a background task."""
        if self._running:
            return
        
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("CompensationService started")
    
    async def stop(self) -> None:
        """Stop the compensation service."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("CompensationService stopped")
    
    async def _run_loop(self) -> None:
        """Main compensation loop."""
        while self._running:
            try:
                await self._scan_and_repair()
                await asyncio.sleep(self._scan_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("CompensationService loop error: %s", e)
                await asyncio.sleep(min(300, self._scan_interval * 2))
    
    async def _scan_and_repair(self) -> None:
        """Scan for inconsistencies and repair them."""
        # 1. Scan pending transactions in Redis
        pending_txs = await self._scan_pending_transactions()
        for tx in pending_txs:
            await self._repair_transaction(tx)
        
        # 2. Scan MySQL tasks missing in Redis
        missing_tasks = await self._scan_missing_redis_tasks()
        for task in missing_tasks:
            await self._repair_missing_task(task)
        
        if pending_txs or missing_tasks:
            logger.info(
                "CompensationService repaired %d txs, %d tasks",
                len(pending_txs), len(missing_tasks),
            )
    
    async def _scan_pending_transactions(self) -> List[TxRecord]:
        """Scan Redis for pending transactions."""
        if not self._redis:
            return []
        
        try:
            # Scan for tx:* keys
            tx_keys = []
            cursor = 0
            while True:
                cursor, keys = await self._redis.scan(
                    cursor=cursor, match="tx:*", count=self._batch_size
                )
                tx_keys.extend(keys)
                if cursor == 0:
                    break
            
            txs = []
            for key in tx_keys:
                try:
                    data = await self._redis.get(key)
                    if not data:
                        continue
                    
                    tx_dict = json.loads(data)
                    tx = TxRecord(
                        tx_id=tx_dict["tx_id"],
                        task_id=tx_dict["task_id"],
                        operation=tx_dict["operation"],
                        status=TxStatus(tx_dict["status"]),
                        mysql_written=tx_dict.get("mysql_written", False),
                        redis_written=tx_dict.get("redis_written", False),
                        created_at=tx_dict["created_at"],
                        retry_count=tx_dict.get("retry_count", 0),
                        last_error=tx_dict.get("last_error", ""),
                        payload=tx_dict.get("payload", {}),
                    )
                    txs.append(tx)
                except Exception as e:
                    logger.warning("Failed to parse tx %s: %s", key, e)
            
            return [tx for tx in txs if tx.status in (TxStatus.PENDING_REDIS, TxStatus.FAILED)]
        
        except Exception as e:
            logger.error("Failed to scan pending transactions: %s", e)
            return []
    
    async def _scan_missing_redis_tasks(self) -> List[TaskRecord]:
        """Scan MySQL for tasks that should be in Redis but aren't.
        
        Criteria:
        - Status is QUEUED, RUNNING, or SCHEDULED (active tasks)
        - Updated within last 24 hours
        - Not present in Redis
        """
        async with self._db_factory() as session:
            # Find active tasks
            stmt = select(TaskRecord).where(
                TaskRecord.status.in_([
                    TaskStatus.QUEUED,
                    TaskStatus.RUNNING,
                    TaskStatus.SCHEDULED,
                ]),
                TaskRecord.updated_at >= datetime.now(timezone.utc) - timedelta(hours=24),
            ).limit(self._batch_size)
            
            result = await session.execute(stmt)
            tasks = result.scalars().all()
            
            if not tasks:
                return []

            # Batch EXISTS check via pipeline — O(1) round trips instead of O(n)
            pipe = self._redis.pipeline()
            for task in tasks:
                pipe.exists(f"task:{task.task_id}")
            exists_results = await pipe.execute()

            missing = [
                task for task, exists in zip(tasks, exists_results) if not exists
            ]
            return missing
    
    async def _repair_transaction(self, tx: TxRecord) -> None:
        """Retry a failed transaction."""
        if tx.retry_count >= self._max_retries:
            logger.warning(
                "Transaction %s exceeded max retries (%d), marking expired",
                tx.tx_id, self._max_retries,
            )
            await self._mark_expired(tx)
            return
        
        try:
            # Based on operation type, retry the Redis write
            if tx.operation == "create_task":
                await self._retry_create_task(tx)
            elif tx.operation == "update_status":
                await self._retry_update_status(tx)
            else:
                logger.warning("Unknown operation %s for tx %s", tx.operation, tx.tx_id)
                await self._mark_expired(tx)
        
        except Exception as e:
            logger.error("Failed to repair tx %s: %s", tx.tx_id, e)
            tx.retry_count += 1
            tx.last_error = str(e)
            await self._save_transaction(tx)
    
    async def _retry_create_task(self, tx: TxRecord) -> None:
        """Retry create_task transaction."""
        task_id = tx.task_id
        
        # Fetch task from MySQL
        async with self._db_factory() as session:
            stmt = select(TaskRecord).where(TaskRecord.task_id == task_id)
            result = await session.execute(stmt)
            task = result.scalar_one_or_none()
            
            if not task:
                logger.error("Task %s not found in MySQL for tx %s", task_id, tx.tx_id)
                await self._mark_expired(tx)
                return
            
            # Recreate Redis record
            task_key = f"task:{task_id}"
            task_data = {
                "task_id": task.task_id,
                "tenant_id": task.tenant_id,
                "status": task.status.value,
                "created_at_ts": task.created_at.timestamp(),
                "capability": task.task_type,  # Assuming capability = task_type
                "priority_rank": 3,  # Default priority
            }
            
            await self._redis.set(task_key, json.dumps(task_data), ex=86400)
            
            # Mark as committed
            tx.status = TxStatus.COMMITTED
            tx.redis_written = True
            await self._save_transaction(tx)
            logger.info("Repaired create_task for %s", task_id)
    
    async def _retry_update_status(self, tx: TxRecord) -> None:
        """Retry update_status transaction."""
        task_id = tx.task_id
        
        # Fetch task from MySQL
        async with self._db_factory() as session:
            stmt = select(TaskRecord).where(TaskRecord.task_id == task_id)
            result = await session.execute(stmt)
            task = result.scalar_one_or_none()
            
            if not task:
                logger.error("Task %s not found in MySQL for tx %s", task_id, tx.tx_id)
                await self._mark_expired(tx)
                return
            
            # Update Redis status
            task_key = f"task:{task_id}"
            data = await self._redis.get(task_key)
            if data:
                task_data = json.loads(data)
                task_data["status"] = task.status.value
                await self._redis.set(task_key, json.dumps(task_data), ex=86400)
            
            # Mark as committed
            tx.status = TxStatus.COMMITTED
            tx.redis_written = True
            await self._save_transaction(tx)
            logger.info("Repaired update_status for %s", task_id)
    
    async def _repair_missing_task(self, task: TaskRecord) -> None:
        """Repair a task missing from Redis."""
        task_key = f"task:{task.task_id}"
        
        try:
            # Recreate Redis record
            task_data = {
                "task_id": task.task_id,
                "tenant_id": task.tenant_id,
                "status": task.status.value,
                "created_at_ts": task.created_at.timestamp(),
                "capability": task.task_type,
                "priority_rank": 3,  # Default priority
            }
            
            await self._redis.set(task_key, json.dumps(task_data), ex=86400)
            logger.info("Repaired missing Redis task %s", task.task_id)
        
        except Exception as e:
            logger.error("Failed to repair missing task %s: %s", task.task_id, e)
    
    async def _save_transaction(self, tx: TxRecord) -> None:
        """Save transaction state to Redis."""
        if not self._redis:
            return
        
        tx_key = f"tx:{tx.tx_id}"
        tx_data = json.dumps({
            "tx_id": tx.tx_id,
            "task_id": tx.task_id,
            "operation": tx.operation,
            "status": tx.status.value,
            "mysql_written": tx.mysql_written,
            "redis_written": tx.redis_written,
            "created_at": tx.created_at,
            "retry_count": tx.retry_count,
            "last_error": tx.last_error,
            "payload": tx.payload,
        })
        
        if tx.status == TxStatus.COMMITTED:
            # Remove committed transactions
            await self._redis.delete(tx_key)
        elif tx.status == TxStatus.EXPIRED:
            # Remove expired transactions
            await self._redis.delete(tx_key)
        else:
            # Update with TTL
            await self._redis.set(tx_key, tx_data, ex=300)  # 5 minutes
    
    async def _mark_expired(self, tx: TxRecord) -> None:
        """Mark transaction as expired and remove it."""
        tx.status = TxStatus.EXPIRED
        await self._save_transaction(tx)
    
    async def enqueue(self, tx: TxRecord) -> None:
        """Enqueue a transaction for compensation."""
        await self._save_transaction(tx)
        logger.debug("Enqueued tx %s for compensation", tx.tx_id)