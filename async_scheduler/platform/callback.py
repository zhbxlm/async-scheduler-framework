"""Callback dispatch with two-level retry and persistent dead-letter queue.

Level 1 (in-memory):
    Up to ``max_inline_attempts`` exponential-backoff retries in the same
    coroutine (handles transient network hiccups, ~seconds).

Level 2 (persistent retry queue backed by Redis ZSET):
    On L1 exhaustion the event is written to ``callback:retry:pending`` with
    score = next-retry-timestamp.  A background loop (``process_due_callbacks``)
    periodically pops due events and retries them.  Events that exceed
    ``max_persistent_attempts`` are moved to ``callback:dead_letter`` (kept
    for ``dead_letter_ttl_seconds``).

This matches the deepwiki ray-async TaskCompletionNode two-level retry spec.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

try:
    import httpx

    _HTTPX_AVAILABLE = True
except ImportError:
    _HTTPX_AVAILABLE = False

logger = logging.getLogger(__name__)

# Redis key constants
_RETRY_QUEUE_KEY = "callback:retry:pending"    # ZSET  score = next_retry_ts
_DONE_KEY_PREFIX = "callback:done:"             # string flag per task_id
_DLQ_KEY_PREFIX = "callback:dlq:"              # string per event_id in dead-letter
_DONE_TTL = 86400 * 7                           # 7 days
_DLQ_TTL = 86400 * 30                           # 30 days


@dataclass
class CallbackEvent:
    """An outbound callback event stored in the persistent retry queue."""

    event_id: str
    task_id: str
    callback_url: str
    payload: dict[str, Any]
    attempt: int = 0
    created_at: float = field(default_factory=time.time)
    next_retry_at: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> "CallbackEvent":
        return cls(**json.loads(raw))


class CallbackDispatcher:
    """Dispatch HTTP callbacks with two-level retry semantics.

    Parameters
    ----------
    max_inline_attempts:
        Level-1 retry count inside the calling coroutine.
    inline_base_delay_seconds:
        Base delay for L1 exponential backoff.
    max_persistent_attempts:
        Maximum total attempts across L1 + L2 before moving to DLQ.
    persistent_base_delay_seconds:
        Base delay for L2 exponential backoff.
    persistent_max_delay_seconds:
        Upper bound on L2 retry delays.
    redis_client:
        Optional async Redis client.  When provided, failed callbacks are
        written to the persistent retry ZSET instead of being silently dropped.
    request_timeout_seconds:
        Per-request HTTP timeout.
    idempotency_header:
        Header name for idempotency key (default: ``Idempotency-Key``).
    """

    def __init__(
        self,
        max_inline_attempts: int = 3,
        inline_base_delay_seconds: float = 2.0,
        max_persistent_attempts: int = 10,
        persistent_base_delay_seconds: float = 30.0,
        persistent_max_delay_seconds: float = 3600.0,
        redis_client: Any = None,
        request_timeout_seconds: float = 10.0,
        idempotency_header: str = "Idempotency-Key",
        async_proxy_sidecar: Any = None,
        # Legacy compat
        max_attempts: int | None = None,
        retry_delay_seconds: float | None = None,
    ) -> None:
        self.max_inline_attempts = max_attempts if max_attempts is not None else max_inline_attempts
        self.inline_base_delay = retry_delay_seconds if retry_delay_seconds is not None else inline_base_delay_seconds
        self.max_persistent_attempts = max_persistent_attempts
        self.persistent_base_delay = persistent_base_delay_seconds
        self.persistent_max_delay = persistent_max_delay_seconds
        self._redis = redis_client
        self._timeout = request_timeout_seconds
        self._idempotency_header = idempotency_header
        self.async_proxy_sidecar = async_proxy_sidecar
        self._http_client: Any = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def dispatch(
        self,
        callback_url: str | None,
        payload: dict[str, Any],
        *,
        task_id: str | None = None,
        attempt_number: int = 0,
    ) -> bool:
        """Attempt to deliver a callback.

        Returns True on success, False on final failure (event enqueued to L2
        if Redis is available).
        """
        if not callback_url:
            return True

        tid = task_id or payload.get("task_id", "unknown")

        for attempt in range(1, self.max_inline_attempts + 1):
            try:
                ok = await self._send(callback_url, payload, task_id=tid, attempt=attempt + attempt_number)
                if ok:
                    if self._redis is not None:
                        await self._mark_done(tid)
                    return True
            except Exception:
                logger.exception(
                    "callback L1 attempt=%s failed url=%s task_id=%s", attempt, callback_url, tid
                )

            if attempt < self.max_inline_attempts:
                delay = self.inline_base_delay * (2 ** (attempt - 1))
                await asyncio.sleep(delay)

        # L1 exhausted – enqueue to persistent retry if Redis available
        logger.warning(
            "callback L1 exhausted; enqueueing for persistent retry url=%s task_id=%s", callback_url, tid
        )
        if self._redis is not None:
            event = CallbackEvent(
                event_id=str(uuid.uuid4()),
                task_id=tid,
                callback_url=callback_url,
                payload=payload,
                attempt=self.max_inline_attempts + attempt_number,
                next_retry_at=time.time() + self.persistent_base_delay,
            )
            await self._enqueue_retry(event)
            # Publish sidecar requeued event
            await self._publish_requeued_event(event)
        return False

    async def mark_done(self, task_id: str) -> None:
        """Explicitly mark a task's callback as delivered (idempotent)."""
        await self._mark_done(task_id)

    async def is_done(self, task_id: str) -> bool:
        """Return True if the callback for *task_id* has been delivered."""
        if self._redis is None:
            return False
        key = f"{_DONE_KEY_PREFIX}{task_id}"
        val = await self._redis.get(key)
        return val is not None

    async def is_in_retry_queue(self, task_id: str) -> bool:
        """Return True if there is a pending retry event for *task_id*."""
        if self._redis is None:
            return False
        # Scan the ZSET for events belonging to this task_id
        # (small DLQ-safe approach – full scan only in reconciler, not hot path)
        all_raw = await self._redis.zrangebyscore(_RETRY_QUEUE_KEY, "-inf", "+inf")
        for raw in all_raw:
            try:
                evt = CallbackEvent.from_json(raw)
                if evt.task_id == task_id:
                    return True
            except Exception:
                pass
        return False

    async def process_due_callbacks(self, batch_size: int = 50) -> int:
        """Process due retry events.  Call this from a background loop.

        Returns the number of events processed (success + DLQ).
        """
        if self._redis is None:
            return 0

        now = time.time()
        processed = 0

        # Atomically fetch due events (score <= now)
        raw_events: list[str] = await self._redis.zrangebyscore(
            _RETRY_QUEUE_KEY, "-inf", str(now), start=0, num=batch_size
        )

        for raw in raw_events:
            # Atomic remove-before-process to prevent double delivery
            removed = await self._redis.zrem(_RETRY_QUEUE_KEY, raw)
            if not removed:
                continue  # another worker beat us to it

            try:
                event = CallbackEvent.from_json(raw)
            except Exception:
                logger.warning("callback retry: failed to decode event raw=%r", raw)
                processed += 1
                continue

            event.attempt += 1
            ok = False
            try:
                ok = await self._send(
                    event.callback_url, event.payload, task_id=event.task_id, attempt=event.attempt
                )
            except Exception:
                logger.exception("callback L2 attempt=%s failed task_id=%s", event.attempt, event.task_id)

            if ok:
                await self._mark_done(event.task_id)
                logger.info("callback L2 delivered task_id=%s attempt=%s", event.task_id, event.attempt)
            elif event.attempt >= self.max_persistent_attempts:
                await self._move_to_dlq(event)
                logger.error(
                    "callback DLQ task_id=%s attempts=%s url=%s",
                    event.task_id,
                    event.attempt,
                    event.callback_url,
                )
            else:
                # Re-enqueue with exponential backoff
                delay = min(
                    self.persistent_max_delay,
                    self.persistent_base_delay * (2 ** (event.attempt - self.max_inline_attempts - 1)),
                )
                event.next_retry_at = time.time() + delay
                await self._enqueue_retry(event)
                # Publish requeued event
                await self._publish_requeued_event(event)
                logger.info(
                    "callback L2 requeued task_id=%s attempt=%s next_in=%.0fs",
                    event.task_id,
                    event.attempt,
                    delay,
                )

            processed += 1

        return processed

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _send(self, url: str, payload: dict[str, Any], *, task_id: str, attempt: int) -> bool:
        headers = {
            self._idempotency_header: task_id,
            "X-Retry-Attempt": str(attempt),
            "Content-Type": "application/json",
        }
        if _HTTPX_AVAILABLE:
            client = await self._get_http_client()
            try:
                resp = await client.post(url, json=payload, headers=headers, timeout=self._timeout)
                if resp.status_code < 500:
                    return True
                logger.warning("callback non-2xx status=%s url=%s", resp.status_code, url)
                return False
            except Exception:
                raise
        else:
            # Stub mode (no httpx): log and succeed
            logger.info(
                "callback stub mode attempt=%s url=%s task_id=%s payload_keys=%s",
                attempt,
                url,
                task_id,
                list(payload.keys()),
            )
            return True

    async def _get_http_client(self) -> Any:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient()
        return self._http_client

    async def _enqueue_retry(self, event: CallbackEvent) -> None:
        await self._redis.zadd(_RETRY_QUEUE_KEY, {event.to_json(): event.next_retry_at})

    async def _mark_done(self, task_id: str) -> None:
        if self._redis is None:
            return
        key = f"{_DONE_KEY_PREFIX}{task_id}"
        await self._redis.set(key, "1", ex=_DONE_TTL)

    async def _move_to_dlq(self, event: CallbackEvent) -> None:
        key = f"{_DLQ_KEY_PREFIX}{event.event_id}"
        await self._redis.set(key, event.to_json(), ex=_DLQ_TTL)
        # Publish dead-letter event
        await self._publish_dead_letter_event(event)

    async def get_stats(self) -> dict[str, Any]:
        retry_queue_size = 0
        dead_letter_size = 0
        recent_retry_events: list[dict[str, Any]] = []
        recent_dead_letters: list[dict[str, Any]] = []
        if self._redis is not None:
            try:
                retry_raw = await self._redis.zrangebyscore(_RETRY_QUEUE_KEY, "-inf", "+inf")
                retry_queue_size = len(retry_raw)
                for raw in retry_raw[-5:]:
                    try:
                        evt = CallbackEvent.from_json(raw)
                        recent_retry_events.append({
                            "task_id": evt.task_id,
                            "attempt": evt.attempt,
                            "next_retry_at": evt.next_retry_at,
                            "callback_url": evt.callback_url,
                        })
                    except Exception:
                        continue
            except Exception:
                retry_queue_size = 0
            try:
                dlq_keys = await self._redis.keys(f"{_DLQ_KEY_PREFIX}*")
                dead_letter_size = len(dlq_keys)
                for key in dlq_keys[-5:]:
                    try:
                        raw = await self._redis.get(key)
                        if not raw:
                            continue
                        evt = CallbackEvent.from_json(raw)
                        recent_dead_letters.append({
                            "task_id": evt.task_id,
                            "attempt": evt.attempt,
                            "callback_url": evt.callback_url,
                            "event_id": evt.event_id,
                        })
                    except Exception:
                        continue
            except Exception:
                dead_letter_size = 0

        return {
            "redis_backed": self._redis is not None,
            "retry_queue_size": retry_queue_size,
            "dead_letter_size": dead_letter_size,
            "max_inline_attempts": self.max_inline_attempts,
            "max_persistent_attempts": self.max_persistent_attempts,
            "recent_retry_events": recent_retry_events,
            "recent_dead_letters": recent_dead_letters,
        }

    async def close(self) -> None:
        """Close the HTTP client if open."""
        if self._http_client is not None and _HTTPX_AVAILABLE:
            await self._http_client.aclose()
            self._http_client = None

    async def _publish_requeued_event(self, event: CallbackEvent) -> None:
        if self.async_proxy_sidecar is None:
            return
        try:
            from async_scheduler.platform.async_proxy import TaskEvent
            await self.async_proxy_sidecar.publish(
                TaskEvent(
                    task_id=event.task_id,
                    status="callback_requeued",
                    callback_url=event.callback_url,
                    result={
                        "event_id": event.event_id,
                        "attempt": event.attempt,
                        "next_retry_at": event.next_retry_at,
                        "retry_queue_size": await self._get_retry_queue_size(),
                    },
                    timestamp=time.time(),
                )
            )
        except Exception:
            logger.exception("Failed to publish requeued event for task %s", event.task_id)

    async def _publish_dead_letter_event(self, event: CallbackEvent) -> None:
        if self.async_proxy_sidecar is None:
            return
        try:
            from async_scheduler.platform.async_proxy import TaskEvent
            dlq_size = await self._get_dead_letter_size()
            await self.async_proxy_sidecar.publish(
                TaskEvent(
                    task_id=event.task_id,
                    status="callback_dead_letter",
                    callback_url=event.callback_url,
                    result={
                        "event_id": event.event_id,
                        "attempt": event.attempt,
                        "max_attempts": self.max_persistent_attempts,
                        "dead_letter_size": dlq_size,
                    },
                    timestamp=time.time(),
                )
            )
        except Exception:
            logger.exception("Failed to publish dead-letter event for task %s", event.task_id)

    async def _get_retry_queue_size(self) -> int:
        if self._redis is None:
            return 0
        try:
            return await self._redis.zcard(_RETRY_QUEUE_KEY)
        except Exception:
            return 0

    async def _get_dead_letter_size(self) -> int:
        if self._redis is None:
            return 0
        try:
            # Count keys matching the DLQ pattern
            pattern = f"{_DLQ_KEY_PREFIX}*"
            keys = await self._redis.keys(pattern)
            return len(keys)
        except Exception:
            return 0
