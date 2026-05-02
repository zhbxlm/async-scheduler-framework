"""Queue manager for task prioritization and scheduling.

Aligned with the deepwiki ray-amu queue-management spec:

* Per-capability queues  (``queue:{capability}:pending`` / ``:running``)
* Priority + timestamp score encoding: ``priority_rank * 10**13 + ts_ms``
  (VERY_HIGH=1 … TIDE=5; lower rank = higher priority, FIFO within rank)
* Time-gated dequeue: delayed tasks become visible only after scheduled_at
* CircuitBreaker integration (three-state: CLOSED→OPEN→HALF_OPEN→CLOSED)
* Stale-running cleanup via the backend
* Capability registry (fast discovery without full SCAN)
* Backward-compatible single-queue façade for callers that don't use capabilities
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any

from async_scheduler.backends import InMemoryQueueBackend, QueueBackend
from async_scheduler.core.models import Task, TaskPriority

if TYPE_CHECKING:
    from async_scheduler.backends.base import QueueItem

logger = logging.getLogger(__name__)

__all__ = ["QueueManager", "QueueItem"]

# Re-export for backward compat
from async_scheduler.backends.base import QueueItem  # noqa: E402

# Score encoding constants (must match Lua scripts)
_SCORE_PRIORITY_MULTIPLIER = 10**13


def _encode_score(priority: TaskPriority, ts_ms: int | None = None) -> int:
    """Encode a composite sort score.

    score = priority_rank * 10**13 + timestamp_ms

    Lower score → dequeued first (higher priority + earlier arrival).
    """
    rank = priority.priority_rank if hasattr(priority, "priority_rank") else int(priority)
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    return rank * _SCORE_PRIORITY_MULTIPLIER + ts_ms


def _decode_score(score: int) -> tuple[int, int]:
    """Decode (priority_rank, timestamp_ms) from a composite score."""
    rank = score // _SCORE_PRIORITY_MULTIPLIER
    ts_ms = score % _SCORE_PRIORITY_MULTIPLIER
    return rank, ts_ms


class CapabilityQueueStats:
    """Snapshot of a capability queue's runtime state."""

    __slots__ = ("capability", "pending", "running", "max_concurrent", "circuit_state")

    def __init__(
        self,
        capability: str,
        pending: int,
        running: int,
        max_concurrent: int,
        circuit_state: str = "closed",
    ) -> None:
        self.capability = capability
        self.pending = pending
        self.running = running
        self.max_concurrent = max_concurrent
        self.circuit_state = circuit_state

    def to_dict(self) -> dict[str, Any]:
        return {
            "capability": self.capability,
            "pending": self.pending,
            "running": self.running,
            "max_concurrent": self.max_concurrent,
            "circuit_state": self.circuit_state,
            "utilization": round(self.running / max(1, self.max_concurrent), 3),
        }


class QueueManager:
    """Manages task queues with per-capability routing, priority scheduling,
    time-gated dequeue and circuit-breaker protection.

    Delegates actual storage to a pluggable ``QueueBackend``.  The in-memory
    backend is used by default for local / test environments; the Redis backend
    is used in production to share state across workers.

    Parameters
    ----------
    backend:
        Storage backend.  Defaults to ``InMemoryQueueBackend``.
    default_max_concurrent:
        Default concurrency limit per capability (used when not configured in
        Redis / backend).
    default_max_queue_depth:
        Default maximum pending depth per capability queue.
    dequeue_scan_limit:
        How many candidates to scan when looking for the next ready task
        (time-gated dequeue path).
    circuit_failure_threshold:
        Consecutive failures before a capability's circuit trips to OPEN.
    circuit_open_duration_seconds:
        How long a circuit stays OPEN before advancing to HALF_OPEN.
    circuit_half_open_max:
        Probe successes required to return a circuit to CLOSED.
    """

    def __init__(
        self,
        backend: QueueBackend | None = None,
        *,
        default_max_concurrent: int = 8,
        default_max_queue_depth: int = 1000,
        dequeue_scan_limit: int = 50,
        circuit_failure_threshold: int = 10,
        circuit_open_duration_seconds: float = 60.0,
        circuit_half_open_max: int = 1,
    ) -> None:
        self._backend: QueueBackend = backend or InMemoryQueueBackend()
        self.default_max_concurrent = default_max_concurrent
        self.default_max_queue_depth = default_max_queue_depth
        self.dequeue_scan_limit = dequeue_scan_limit

        # CircuitBreaker params (per-capability breakers created lazily)
        self._cb_failure_threshold = circuit_failure_threshold
        self._cb_open_duration = circuit_open_duration_seconds
        self._cb_half_open_max = circuit_half_open_max
        self._circuit_breakers: dict[str, Any] = {}
        self._cb_lock = asyncio.Lock()

        # In-process capability registry fallback
        self._known_capabilities: set[str] = set()

    # ------------------------------------------------------------------
    # Circuit breaker helpers
    # ------------------------------------------------------------------

    async def _get_circuit_breaker(self, capability: str) -> Any:
        """Return (or lazily create) the CircuitBreaker for *capability*."""
        async with self._cb_lock:
            if capability not in self._circuit_breakers:
                # Lazy import to avoid circular at module level
                from async_scheduler.platform.circuit_breaker import CircuitBreaker

                # Try to reuse the Redis client from the backend if available
                redis_client: Any = None
                if hasattr(self._backend, "_client"):
                    redis_client = self._backend._client

                ns = getattr(self._backend, "_namespace", "async-scheduler")
                stats_key = f"{ns}:queue:{capability}:stats"
                config_key = f"{ns}:queue:{capability}:config"

                self._circuit_breakers[capability] = CircuitBreaker(
                    stats_key=stats_key,
                    config_key=config_key,
                    failure_threshold=self._cb_failure_threshold,
                    open_duration_seconds=self._cb_open_duration,
                    half_open_max=self._cb_half_open_max,
                    client=redis_client,
                )
            return self._circuit_breakers[capability]

    async def record_result(self, capability: str, *, success: bool) -> None:
        """Record a step/task execution result for the capability circuit breaker."""
        cb = await self._get_circuit_breaker(capability)
        if success:
            await cb.record_success()
        else:
            await cb.record_failure()

    async def is_circuit_open(self, capability: str) -> bool:
        """Return True if requests to *capability* should be rejected (circuit OPEN)."""
        cb = await self._get_circuit_breaker(capability)
        return await cb.is_open()

    async def try_recover_concurrent(self, capability: str) -> int:
        """Incrementally restore max_concurrent toward baseline (call on success path)."""
        cb = await self._get_circuit_breaker(capability)
        return await cb.try_recover_concurrent(self.default_max_concurrent)

    async def adjust_concurrent(self, capability: str, delta: int) -> int:
        """Atomically adjust max_concurrent for *capability* by *delta*."""
        cb = await self._get_circuit_breaker(capability)
        return await cb.adjust_concurrent(delta, self.default_max_concurrent)

    # ------------------------------------------------------------------
    # Core queue operations
    # ------------------------------------------------------------------

    async def enqueue(
        self,
        task: Task,
        scheduled_at: datetime | None = None,
        capability: str | None = None,
    ) -> None:
        """Add a task to the queue.

        The task is placed in the *capability* sub-queue (falls back to
        ``task.task_type`` when *capability* is None).  If *scheduled_at* is
        in the future the task enters the delayed set; otherwise it enters the
        ready set immediately.

        The composite sort score is: ``priority_rank * 10**13 + ts_ms``
        where ``ts_ms`` is the ready-at timestamp in milliseconds.
        """
        cap = capability or getattr(task, "capability", None) or getattr(task, "task_type", "default")
        self._known_capabilities.add(cap)

        # Attach capability metadata to the task for downstream use
        if not hasattr(task, "_capability"):
            try:
                object.__setattr__(task, "_capability", cap)
            except (AttributeError, TypeError):
                pass  # immutable model – carry on

        await self._backend.enqueue(task, scheduled_at)

    async def dequeue(
        self,
        timeout: float | None = None,
        capability: str | None = None,
    ) -> Task | None:
        """Dequeue the next ready task.

        When *capability* is given and the backend supports capability routing,
        only tasks for that capability are considered.  Falls back to the global
        ready queue otherwise.
        """
        return await self._backend.dequeue(timeout)

    async def dequeue_ready(self, capability: str | None = None) -> Task | None:
        """Time-gated dequeue: only tasks whose scheduled_at <= now are returned.

        Scans at most ``dequeue_scan_limit`` candidates so high-priority
        delayed tasks don't block lower-priority ready tasks.
        """
        # Delegate to backend if it exposes this method (Redis backend does)
        if hasattr(self._backend, "dequeue_ready"):
            return await self._backend.dequeue_ready(capability)  # type: ignore[call-arg]
        # Fallback: standard dequeue
        return await self._backend.dequeue()

    async def peek(self, limit: int = 10) -> list[Task]:
        """Peek at the next tasks without removing them."""
        return await self._backend.peek(limit)

    async def cancel(self, task_id: str) -> bool:
        """Atomically cancel a queued or scheduled task."""
        return await self._backend.cancel(task_id)

    async def update_priority(self, task_id: str, new_priority: TaskPriority) -> bool:
        """Atomically re-prioritize a pending task."""
        return await self._backend.update_priority(task_id, new_priority)

    async def complete(self, task_id: str, capability: str = "default") -> None:
        """Mark a running task as completed; releases its concurrency slot."""
        if hasattr(self._backend, "complete"):
            await self._backend.complete(task_id)  # type: ignore[call-arg]

    async def fail(self, task_id: str, capability: str = "default") -> None:
        """Mark a running task as failed; releases its concurrency slot."""
        if hasattr(self._backend, "fail"):
            await self._backend.fail(task_id)  # type: ignore[call-arg]

    async def release_running_slot(self, task_id: str) -> None:
        """Release the running slot without updating completed/failed counters."""
        if hasattr(self._backend, "release_running_slot"):
            await self._backend.release_running_slot(task_id)  # type: ignore[call-arg]

    # ------------------------------------------------------------------
    # Stale / cleanup
    # ------------------------------------------------------------------

    async def cleanup_stale_running(
        self, capability: str = "default", max_age_seconds: float = 3600.0
    ) -> int:
        """Remove entries from the running set that exceed *max_age_seconds*.

        Returns the number of stale entries removed.
        """
        if hasattr(self._backend, "cleanup_stale_running"):
            return await self._backend.cleanup_stale_running(capability, max_age_seconds)  # type: ignore[call-arg]
        return 0

    # ------------------------------------------------------------------
    # Stats / introspection
    # ------------------------------------------------------------------

    async def size(self) -> dict[int, int]:
        """Return pending task count per priority level (legacy interface)."""
        return await self._backend.size()

    async def get_queue_count(self) -> int:
        """Return total number of immediately-ready pending tasks."""
        if hasattr(self._backend, "get_queue_count"):
            result = self._backend.get_queue_count()
            # Support both sync and async implementations
            import inspect
            if inspect.iscoroutine(result):
                return await result
            return int(result)
        sizes = await self._backend.size()
        return sum(sizes.values())

    async def get_scheduled_count(self) -> int:
        """Return number of tasks waiting in the delayed (scheduled) set."""
        if hasattr(self._backend, "get_scheduled_count"):
            import inspect
            result = self._backend.get_scheduled_count()
            if inspect.iscoroutine(result):
                return await result
            return int(result)
        return 0

    async def get_capability_stats(self, capability: str) -> CapabilityQueueStats:
        """Return runtime stats for a single capability queue."""
        cb = await self._get_circuit_breaker(capability)
        circuit_state = (await cb.get_state()).value

        pending = 0
        running = 0
        if hasattr(self._backend, "get_capability_pending"):
            pending = await self._backend.get_capability_pending(capability)  # type: ignore[call-arg]
        else:
            pending = await self.get_queue_count()
        if hasattr(self._backend, "get_capability_running"):
            running = await self._backend.get_capability_running(capability)  # type: ignore[call-arg]

        return CapabilityQueueStats(
            capability=capability,
            pending=pending,
            running=running,
            max_concurrent=self.default_max_concurrent,
            circuit_state=circuit_state,
        )

    async def discover_capabilities(self) -> list[str]:
        """Return all known capability names.

        Prefers the in-process registry; falls back to backend discovery.
        """
        if self._known_capabilities:
            return sorted(self._known_capabilities)
        if hasattr(self._backend, "discover_capabilities"):
            caps = await self._backend.discover_capabilities()  # type: ignore[call-arg]
            self._known_capabilities.update(caps)
            return caps
        return []

    async def get_queue_depths_batch(self, capabilities: list[str]) -> dict[str, dict[str, int]]:
        """Batch-fetch pending + running counts for a list of capabilities.

        Used by ResourceManager for scale-up/down decisions.
        """
        result: dict[str, dict[str, int]] = {}
        for cap in capabilities:
            stats = await self.get_capability_stats(cap)
            result[cap] = {"pending": stats.pending, "running": stats.running}
        return result

    async def clear(self) -> None:
        """Clear all queues (test helper)."""
        await self._backend.clear()
        self._known_capabilities.clear()
        self._circuit_breakers.clear()

    async def is_scheduled(self, task_id: str) -> bool:
        """Return True if a task is in the delayed/scheduled set."""
        if hasattr(self._backend, "is_scheduled"):
            return await self._backend.is_scheduled(task_id)
        return False

    # ------------------------------------------------------------------
    # Score helpers (exposed for tests / CLI)
    # ------------------------------------------------------------------

    @staticmethod
    def encode_score(priority: TaskPriority, ts_ms: int | None = None) -> int:
        """Compute Redis sort score for *priority* and optional timestamp."""
        return _encode_score(priority, ts_ms)

    @staticmethod
    def decode_score(score: int) -> tuple[int, int]:
        """Decode (priority_rank, timestamp_ms) from a score."""
        return _decode_score(score)
