"""TaskCompletionNode — aligned with docs/deepwiki-reference/任务执行.md

Finalize tasks:
1. Persist final state to MySQL (TaskRecord upsert)
2. Send HTTP callback with 2-level retry:
   - Level-1: in-memory retry (3× exponential backoff)
   - Level-2: Redis ZSET durable retry queue (process_due_callbacks)
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_CALLBACK_RETRY_KEY = "callback:retry:pending"   # ZSET: score=next_retry_ts
_CALLBACK_DONE_KEY = "callback:done:{task_id}"   # SET NX to mark sent
_CALLBACK_DLQ_KEY = "callback:dlq"               # ZSET: dead-letter queue
_MAX_INLINE_RETRIES = 3
_MAX_DURABLE_ATTEMPTS = 8
_BASE_RETRY_DELAY = 10    # seconds
_MAX_RETRY_DELAY = 600    # seconds
_DLQ_TTL = 30 * 86400     # 30 days


class TaskCompletionNode:
    """Handle task finalization: MySQL persist + HTTP callback (2-level retry)."""

    def __init__(
        self,
        db_session_factory: Any | None = None,
        redis_client: Any | None = None,
        *,
        http_timeout: float = 30.0,
        inline_retries: int = _MAX_INLINE_RETRIES,
        max_durable_attempts: int = _MAX_DURABLE_ATTEMPTS,
    ) -> None:
        self._db = db_session_factory
        self._r = redis_client
        self._http_timeout = http_timeout
        self._inline_retries = inline_retries
        self._max_durable_attempts = max_durable_attempts
        self._client: httpx.AsyncClient | None = None
        self._client_lock = asyncio.Lock()

    async def handle_completion(
        self,
        task_id: str,
        status: str,
        result: Any = None,
        *,
        callback_url: str = "",
        tenant_id: str = "",
    ) -> bool:
        """Persist to MySQL + send HTTP callback in parallel."""
        persist_task = asyncio.create_task(
            self._persist(task_id, status, result, tenant_id=tenant_id)
        )
        callback_task = asyncio.create_task(
            self._trigger_callback(task_id, status, result, callback_url=callback_url)
        )
        results = await asyncio.gather(persist_task, callback_task, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                logger.error("TaskCompletionNode: partial failure task_id=%s: %s", task_id, r)
        return True

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    async def _persist(
        self, task_id: str, status: str, result: Any, *, tenant_id: str = ""
    ) -> None:
        """Write final state to MySQL via db_session_factory."""
        if self._db is None:
            logger.debug("TaskCompletionNode: no db_session_factory, skip persist task_id=%s", task_id)
            return
        try:
            async with self._db() as session:
                from sqlalchemy import select, update
                from src.models.task import TaskRecord

                stmt = select(TaskRecord).where(TaskRecord.task_id == task_id)
                row = (await session.execute(stmt)).scalar_one_or_none()
                result_json = json.dumps(result, ensure_ascii=False) if result is not None else None
                error_msg = None
                if status == "failed" and isinstance(result, dict):
                    error_msg = str(result.get("error", ""))
                    if error_msg:
                        error_msg = error_msg[:2000]  # truncate to 2000 chars
                if row:
                    update_values = {"status": status, "output_data": result_json}
                    if error_msg:
                        update_values["error_message"] = error_msg
                    await session.execute(
                        update(TaskRecord)
                        .where(TaskRecord.task_id == task_id)
                        .values(**update_values)
                    )
                else:
                    create_values = {
                        "task_id": task_id,
                        "tenant_id": tenant_id,
                        "status": status,
                        "output_data": result_json,
                    }
                    if error_msg:
                        create_values["error_message"] = error_msg
                    session.add(TaskRecord(**create_values))
                await session.commit()
                logger.info("TaskCompletionNode: persisted task_id=%s status=%s", task_id, status)
        except Exception as exc:
            logger.error("TaskCompletionNode: persist failed task_id=%s: %s", task_id, exc)
            raise

    # ------------------------------------------------------------------
    # HTTP Callback — Level-1 (inline retry)
    # ------------------------------------------------------------------

    async def _trigger_callback(
        self,
        task_id: str,
        status: str,
        result: Any,
        *,
        callback_url: str = "",
    ) -> None:
        if not callback_url:
            return

        payload = {
            "task_id": task_id,
            "status": status,
            "result": result,
            "timestamp": int(time.time()),
        }

        for attempt in range(1, self._inline_retries + 1):
            try:
                client = await self._get_client()
                resp = await client.post(
                    callback_url,
                    json=payload,
                    headers={
                        "Idempotency-Key": task_id,
                        "X-Retry-Attempt": str(attempt),
                    },
                )
                resp.raise_for_status()
                if self._r:
                    await self._r.set(
                        _CALLBACK_DONE_KEY.format(task_id=task_id), "1", ex=86400
                    )
                logger.info("TaskCompletionNode: callback sent task_id=%s attempt=%d", task_id, attempt)
                return
            except Exception as exc:
                logger.warning(
                    "TaskCompletionNode: callback attempt=%d failed task_id=%s: %s",
                    attempt, task_id, exc,
                )
                if attempt < self._inline_retries:
                    await asyncio.sleep(2 ** attempt)

        # Level-2: enqueue to durable retry queue
        await self._enqueue_callback_retry(task_id, callback_url, payload, attempt=1)

    # ------------------------------------------------------------------
    # HTTP Callback — Level-2 (durable Redis ZSET retry)
    # ------------------------------------------------------------------

    async def _enqueue_callback_retry(
        self,
        task_id: str,
        callback_url: str,
        payload: dict,
        attempt: int = 1,
    ) -> None:
        if self._r is None:
            return
        delay = min(_MAX_RETRY_DELAY, _BASE_RETRY_DELAY * (2 ** (attempt - 1)))
        next_retry_ts = time.time() + delay
        event = json.dumps({
            "task_id": task_id,
            "callback_url": callback_url,
            "payload": payload,
            "attempt": attempt,
        })
        await self._r.zadd(_CALLBACK_RETRY_KEY, {event: next_retry_ts})
        logger.info(
            "TaskCompletionNode: enqueued durable retry task_id=%s attempt=%d delay=%ds",
            task_id, attempt, delay,
        )

    async def process_due_callbacks(self, batch_size: int = 50) -> int:
        """Process due callback events from the durable retry queue."""
        if self._r is None:
            return 0

        now = time.time()
        events = await self._r.zrangebyscore(
            _CALLBACK_RETRY_KEY, "-inf", now, start=0, num=batch_size, withscores=True
        )
        if not events:
            return 0

        processed = 0
        for raw_event, score in events:
            event_str = raw_event.decode() if isinstance(raw_event, bytes) else raw_event
            # Atomic pop to prevent concurrent processing
            removed = await self._r.zrem(_CALLBACK_RETRY_KEY, raw_event)
            if not removed:
                continue

            try:
                event = json.loads(event_str)
            except json.JSONDecodeError:
                continue

            task_id = event.get("task_id", "")
            callback_url = event.get("callback_url", "")
            payload = event.get("payload", {})
            attempt = event.get("attempt", 1)

            success = await self._send_callback_once(callback_url, payload, task_id, attempt)
            if success:
                if self._r:
                    await self._r.set(
                        _CALLBACK_DONE_KEY.format(task_id=task_id), "1", ex=86400
                    )
            elif attempt < self._max_durable_attempts:
                await self._enqueue_callback_retry(task_id, callback_url, payload, attempt + 1)
            else:
                # Dead-letter queue
                await self._r.zadd(_CALLBACK_DLQ_KEY, {event_str: time.time()})
                await self._r.expire(_CALLBACK_DLQ_KEY, _DLQ_TTL)
                logger.error(
                    "TaskCompletionNode: DLQ task_id=%s after %d attempts", task_id, attempt
                )
            processed += 1

        return processed

    async def _send_callback_once(
        self, url: str, payload: dict, task_id: str, attempt: int
    ) -> bool:
        try:
            client = await self._get_client()
            resp = await client.post(
                url,
                json=payload,
                headers={"Idempotency-Key": task_id, "X-Retry-Attempt": str(attempt)},
            )
            resp.raise_for_status()
            return True
        except Exception as exc:
            logger.warning("TaskCompletionNode: durable send failed task_id=%s: %s", task_id, exc)
            return False

    # ------------------------------------------------------------------
    # HTTP client (shared, loop-aware)
    # ------------------------------------------------------------------

    async def _get_client(self) -> httpx.AsyncClient:
        async with self._client_lock:
            loop = asyncio.get_event_loop()
            if (
                self._client is None
                or self._client.is_closed
                or getattr(self._client, "_loop", loop) is not loop
            ):
                if self._client and not self._client.is_closed:
                    await self._client.aclose()
                self._client = httpx.AsyncClient(timeout=self._http_timeout)
                self._client._loop = loop  # type: ignore[attr-defined]
            return self._client

    async def close(self) -> None:
        async with self._client_lock:
            if self._client and not self._client.is_closed:
                await self._client.aclose()
                self._client = None
