"""Domain event bus for decoupled inter-service communication.

Instead of services calling each other directly, they publish events
that interested parties subscribe to. This eliminates circular imports
and makes the system easier to extend without modifying existing code.

Usage:
    from src.common.events import event_bus, Event

    # Publisher (e.g. TaskCreator)
    await event_bus.publish(Event("task.created", {"task_id": "...", "priority": "normal"}))

    # Subscriber (e.g. QuotaEnforcer)
    @event_bus.on("task.created")
    async def handle_task_created(event: Event) -> None:
        await quota_enforcer.increment(event.data["tenant_id"])
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)

Handler = Callable[["Event"], Awaitable[None]]


@dataclass
class Event:
    """Immutable domain event."""
    topic: str
    data: dict[str, Any] = field(default_factory=dict)

    # Well-known topics
    TASK_CREATED   = "task.created"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED    = "task.failed"
    TASK_CANCELLED = "task.cancelled"
    NODE_DEAD      = "node.dead"
    CB_OPENED      = "circuit_breaker.opened"
    CB_CLOSED      = "circuit_breaker.closed"


class EventBus:
    """In-process async event bus (publish-subscribe).

    Thread-safe for asyncio; all handlers run in the same event loop.
    Errors in handlers are logged but never propagate to the publisher.
    """

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Handler]] = {}
        self._wildcard: list[Handler] = []

    def on(self, topic: str):
        """Decorator to register a handler for *topic*.

        Use ``'*'`` to receive all events.
        """
        def decorator(fn: Handler) -> Handler:
            self.subscribe(topic, fn)
            return fn
        return decorator

    def subscribe(self, topic: str, handler: Handler) -> None:
        """Register *handler* to receive events on *topic* (or ``'*'``)."""
        if topic == "*":
            self._wildcard.append(handler)
        else:
            self._subscribers.setdefault(topic, []).append(handler)

    def unsubscribe(self, topic: str, handler: Handler) -> None:
        """Remove a previously registered handler."""
        if topic == "*":
            self._wildcard = [h for h in self._wildcard if h is not handler]
        else:
            handlers = self._subscribers.get(topic, [])
            self._subscribers[topic] = [h for h in handlers if h is not handler]

    async def publish(self, event: Event) -> None:
        """Publish *event* to all registered handlers.

        All handlers are awaited concurrently; individual failures are
        caught and logged so one bad handler cannot block others.
        """
        handlers = [
            *self._subscribers.get(event.topic, []),
            *self._wildcard,
        ]
        if not handlers:
            return

        results = await asyncio.gather(
            *[h(event) for h in handlers],
            return_exceptions=True,
        )
        for h, result in zip(handlers, results):
            if isinstance(result, Exception):
                logger.error(
                    "EventBus: handler %s failed for topic=%s: %s",
                    getattr(h, "__qualname__", repr(h)),
                    event.topic,
                    result,
                )

    def publish_nowait(self, event: Event) -> None:
        """Fire-and-forget publish (schedules coroutine in running loop)."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.publish(event))
        except RuntimeError:
            logger.warning("EventBus.publish_nowait: no running event loop")

    def subscriber_count(self, topic: str | None = None) -> int:
        if topic:
            return len(self._subscribers.get(topic, []))
        return sum(len(v) for v in self._subscribers.values()) + len(self._wildcard)


# Application-level singleton
event_bus = EventBus()
