"""AsyncProxySidecar: Redis Pub/Sub async callback notification for long-running tasks.

Implements G11 from the deepwiki P2 roadmap:
- Acts as a sidecar alongside Flask/WSGI services for 30s+ async tasks
- Publishes task completion/failure events to Redis channel
- Subscribers (clients) can listen for their task's event instead of polling
- In-process fallback: asyncio.Event-based notification when Redis unavailable

Architecture:
  1. Task submitted → returns immediately with task_id
  2. Worker completes task → AsyncProxySidecar.publish(task_id, result)
  3. Client subscribed to channel → receives notification within ms

Channel naming: task:events:{task_id}
Global channel:  task:events:*  (wildcard psubscribe for monitoring)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)

__all__ = ["TaskEvent", "AsyncProxySidecar"]

CHANNEL_PREFIX = "task:events"
GLOBAL_CHANNEL = "task:events:global"


@dataclass
class TaskEvent:
    """Event emitted when a task changes state."""
    task_id: str
    status: str  # "success" | "failed" | "cancelled" | "timeout"
    result: dict[str, Any] | None = None
    error: str | None = None
    worker_id: str | None = None
    timestamp: float = field(default_factory=time.time)
    attempt_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "result": self.result,
            "error": self.error,
            "worker_id": self.worker_id,
            "timestamp": self.timestamp,
            "attempt_id": self.attempt_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TaskEvent":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def serialize(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def deserialize(cls, raw: str) -> "TaskEvent":
        return cls.from_dict(json.loads(raw))


class AsyncProxySidecar:
    """Async proxy for task completion notifications via Redis Pub/Sub.

    Provides two modes:
    1. Redis Pub/Sub: Production mode with real-time cross-process delivery
    2. In-process fallback: asyncio.Event-based for single-process / test use

    Usage::

        sidecar = AsyncProxySidecar(redis_client=redis)
        await sidecar.start()

        # Worker side: publish when task completes
        await sidecar.publish(TaskEvent(task_id="abc", status="success", result={...}))

        # Client side: wait for event with timeout
        event = await sidecar.wait_for(task_id="abc", timeout=60.0)
    """

    def __init__(
        self,
        redis_client: Any | None = None,
        event_ttl_seconds: float = 300.0,
    ) -> None:
        self._redis = redis_client
        self._event_ttl = event_ttl_seconds
        self._in_process_events: dict[str, asyncio.Event] = {}
        self._in_process_results: dict[str, TaskEvent] = {}
        self._subscribers: list[Callable[[TaskEvent], Awaitable[None]]] = []
        self._running = False
        self._pubsub_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the sidecar. Launches Redis Pub/Sub listener if Redis is available."""
        if self._running:
            return
        self._running = True
        if self._redis is not None and hasattr(self._redis, "pubsub"):
            self._pubsub_task = asyncio.create_task(self._pubsub_loop())
        logger.info("AsyncProxySidecar started redis=%s", self._redis is not None)

    async def stop(self) -> None:
        """Stop the sidecar."""
        self._running = False
        if self._pubsub_task:
            self._pubsub_task.cancel()
            try:
                await self._pubsub_task
            except asyncio.CancelledError:
                pass
        logger.info("AsyncProxySidecar stopped")

    async def publish(self, event: TaskEvent) -> None:
        """Publish a task completion event.

        Delivers via Redis Pub/Sub if available, otherwise signals in-process waiters.
        """
        payload = event.serialize()
        channel = f"{CHANNEL_PREFIX}:{event.task_id}"

        # Always update in-process state for local waiters
        self._in_process_results[event.task_id] = event
        local_event = self._in_process_events.get(event.task_id)
        if local_event is not None:
            local_event.set()

        # Notify local subscribers
        for subscriber in self._subscribers:
            try:
                await subscriber(event)
            except Exception as e:
                logger.warning("AsyncProxySidecar subscriber error: %s", e)

        if self._redis is not None:
            try:
                await self._redis.publish(channel, payload)
                await self._redis.publish(GLOBAL_CHANNEL, payload)
                # Also cache in Redis with TTL for late subscribers
                cache_key = f"{CHANNEL_PREFIX}:cache:{event.task_id}"
                await self._redis.setex(cache_key, int(self._event_ttl), payload)
            except Exception as e:
                logger.warning("AsyncProxySidecar Redis publish failed: %s", e)

        logger.debug("AsyncProxySidecar published task_id=%s status=%s", event.task_id, event.status)

    async def wait_for(self, task_id: str, timeout: float = 60.0) -> TaskEvent | None:
        """Wait for a task completion event.

        Checks cache first, then waits for real-time notification.
        Returns None on timeout.
        """
        # Check in-process cache first
        if task_id in self._in_process_results:
            return self._in_process_results[task_id]

        # Check Redis cache for already-published events
        if self._redis is not None:
            cache_key = f"{CHANNEL_PREFIX}:cache:{task_id}"
            try:
                raw = await self._redis.get(cache_key)
                if raw:
                    return TaskEvent.deserialize(raw)
            except Exception:
                pass

        # Set up in-process event for notification
        if task_id not in self._in_process_events:
            self._in_process_events[task_id] = asyncio.Event()
        local_event = self._in_process_events[task_id]

        try:
            await asyncio.wait_for(local_event.wait(), timeout=timeout)
            return self._in_process_results.get(task_id)
        except asyncio.TimeoutError:
            return None
        finally:
            self._in_process_events.pop(task_id, None)

    def subscribe(self, callback: Callable[[TaskEvent], Awaitable[None]]) -> None:
        """Register a callback for all task events."""
        self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable[[TaskEvent], Awaitable[None]]) -> None:
        """Remove a previously registered callback."""
        self._subscribers = [s for s in self._subscribers if s is not callback]

    async def _pubsub_loop(self) -> None:
        """Listen to Redis Pub/Sub and forward events to local subscribers."""
        if self._redis is None:
            return
        try:
            pubsub = self._redis.pubsub()
            await pubsub.subscribe(GLOBAL_CHANNEL)
            logger.info("AsyncProxySidecar subscribed to Redis channel=%s", GLOBAL_CHANNEL)
            async for message in pubsub.listen():
                if not self._running:
                    break
                if message and message.get("type") == "message":
                    try:
                        event = TaskEvent.deserialize(message["data"])
                        # Update in-process state for local waiters
                        self._in_process_results[event.task_id] = event
                        local = self._in_process_events.get(event.task_id)
                        if local:
                            local.set()
                        for subscriber in self._subscribers:
                            try:
                                await subscriber(event)
                            except Exception as e:
                                logger.warning("Subscriber error: %s", e)
                    except Exception as e:
                        logger.warning("AsyncProxySidecar message parse error: %s", e)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("AsyncProxySidecar pubsub_loop error: %s", e, exc_info=True)

    def get_stats(self) -> dict[str, Any]:
        return {
            "running": self._running,
            "redis_connected": self._redis is not None,
            "cached_events": len(self._in_process_results),
            "active_waiters": len(self._in_process_events),
            "subscriber_count": len(self._subscribers),
        }
