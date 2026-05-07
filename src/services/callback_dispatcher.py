from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging

import httpx
from sqlalchemy import select

from src.models.callback_outbox import CallbackOutboxRecord, CallbackDeliveryStatus

logger = logging.getLogger(__name__)


class CallbackDispatchService:
    def __init__(self, db_session_factory, *, poll_interval: float = 5.0, max_attempts: int = 8, http_timeout: float = 30.0):
        self._db = db_session_factory
        self._poll_interval = poll_interval
        self._max_attempts = max_attempts
        self._http_timeout = http_timeout
        self._running = False
        self._task = None
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._client = httpx.AsyncClient(timeout=self._http_timeout)
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _loop(self):
        while self._running:
            await self.process_once()
            await asyncio.sleep(self._poll_interval)

    async def process_once(self) -> int:
        if self._db is None or self._client is None:
            return 0
        now = datetime.now(timezone.utc)
        processed = 0
        async with self._db() as session:
            result = await session.execute(
                select(CallbackOutboxRecord).where(
                    CallbackOutboxRecord.delivery_status == CallbackDeliveryStatus.PENDING,
                    (CallbackOutboxRecord.next_attempt_at.is_(None)) | (CallbackOutboxRecord.next_attempt_at <= now),
                ).limit(50)
            )
            rows = result.scalars().all()
            for row in rows:
                try:
                    resp = await self._client.post(row.callback_url, content=row.payload_json, headers={"Content-Type": "application/json", "Idempotency-Key": row.task_id})
                    resp.raise_for_status()
                    row.delivery_status = CallbackDeliveryStatus.DELIVERED
                    row.attempt_count += 1
                except Exception as exc:
                    row.attempt_count += 1
                    row.last_error = str(exc)
                    if row.attempt_count >= self._max_attempts:
                        row.delivery_status = CallbackDeliveryStatus.DEAD_LETTER
                    else:
                        row.next_attempt_at = now
                processed += 1
            await session.commit()
        return processed
