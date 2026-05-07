"""ActorPoolManager — aligned with docs/deepwiki-reference/调度与资源管理.md

Manages a pool of Worker Actor instances for a single capability:
- Least-Loaded + Round-Robin selection
- Named worker actors: worker_{tenant_id}_{cluster_id}_{capability}_{idx}
- Dynamic resize (scale-up / scale-down with inflight-aware sorting)
- Fault auto-recovery: actor faults → replace + retry once
- Circuit-breaker: >3 replaces within 300s → trip per-actor breaker
- Ray-optional: degrades to asyncio coroutines when Ray not available
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

# Fault-protection constants
_ACTOR_FAULT_REPLACE_WINDOW_SECONDS = 300
_ACTOR_FAULT_MAX_REPLACES_PER_WINDOW = 3

# ---------------------------------------------------------------------------
# Ray import with graceful fallback
# ---------------------------------------------------------------------------
try:
    import ray  # type: ignore[import]
    _RAY_AVAILABLE = True
except ImportError:
    ray = None  # type: ignore[assignment]
    _RAY_AVAILABLE = False


def _is_actor_fault(exc: BaseException) -> bool:
    """Return True if *exc* is a Ray-level actor fault (not a user error)."""
    if not _RAY_AVAILABLE:
        return False
    actor_error_types = []
    for name in ("RayActorError", "ActorDiedError", "GetTimeoutError", "OwnerDiedError"):
        t = getattr(ray.exceptions, name, None)
        if t is not None:
            actor_error_types.append(t)
    return isinstance(exc, tuple(actor_error_types)) if actor_error_types else False


# ---------------------------------------------------------------------------
# Worker handle
# ---------------------------------------------------------------------------

@dataclass
class _WorkerHandle:
    """Wraps a single Worker Actor (Ray handle or asyncio stub)."""
    name: str
    actor: Any                              # Ray ActorHandle or local callable
    inflight: list[Any] = field(default_factory=list)  # ObjectRefs or Futures
    fault_times: list[float] = field(default_factory=list)
    alive: bool = True

    @property
    def pending(self) -> int:
        return len(self.inflight)

    def is_tripped(self) -> bool:
        """Circuit breaker: too many faults in window."""
        now = time.time()
        recent = [t for t in self.fault_times if now - t < _ACTOR_FAULT_REPLACE_WINDOW_SECONDS]
        return len(recent) >= _ACTOR_FAULT_MAX_REPLACES_PER_WINDOW

    def record_fault(self) -> None:
        now = time.time()
        self.fault_times.append(now)
        # Prune stale entries to prevent unbounded list growth on long-running workers.
        cutoff = now - _ACTOR_FAULT_REPLACE_WINDOW_SECONDS
        self.fault_times = [t for t in self.fault_times if t > cutoff]


# ---------------------------------------------------------------------------
# ActorPoolManager
# ---------------------------------------------------------------------------

class ActorPoolManager:
    """Manages a pool of Workers for one capability.

    Parameters
    ----------
    capability:
        Capability name this pool serves.
    cluster_id:
        Cluster identifier (used in worker naming).
    tenant_id:
        Tenant identifier (used in worker naming and namespace).
    target_size:
        Initial (and minimum) pool size.
    max_size:
        Upper bound for dynamic scale-up.
    worker_factory:
        Async callable that creates a new worker given an index.
        Signature: ``async def factory(name: str, idx: int) -> Any``
    redis_client:
        Optional Redis client for persisting pool state.
    """

    def __init__(
        self,
        *,
        capability: str,
        cluster_id: str = "default",
        tenant_id: str = "",
        target_size: int = 1,
        max_size: int = 8,
        worker_factory: Callable[[str, int], Awaitable[Any]] | None = None,
        redis_client: Any = None,
    ) -> None:
        self._capability = capability
        self._cluster_id = cluster_id
        self._tenant_id = tenant_id
        self._target_size = target_size
        self._max_size = max_size
        self._factory = worker_factory
        self._redis = redis_client

        self._workers: list[_WorkerHandle] = []
        self._next_index = 0
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Worker naming
    # ------------------------------------------------------------------

    def _worker_name(self, idx: int) -> str:
        tid = self._tenant_id or "default"
        return f"worker_{tid}_{self._cluster_id}_{self._capability}_{idx}"

    # ------------------------------------------------------------------
    # Initialisation & recovery
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Create or recover workers to reach target_size."""
        async with self._lock:
            for idx in range(self._target_size):
                name = self._worker_name(idx)
                worker = await self._try_recover_or_create(name, idx)
                if worker is not None:
                    self._workers.append(_WorkerHandle(name=name, actor=worker))
        logger.info(
            "ActorPoolManager started cap=%s size=%d", self._capability, len(self._workers)
        )

    async def _try_recover_or_create(self, name: str, idx: int) -> Any | None:
        """Try to get existing actor from Ray, or create a new one."""
        if _RAY_AVAILABLE:
            try:
                handle = ray.get_actor(name)
                logger.debug("ActorPoolManager: recovered worker %s", name)
                return handle
            except Exception:
                pass
        if self._factory is not None:
            try:
                return await self._factory(name, idx)
            except Exception as e:
                logger.error("ActorPoolManager: failed to create worker %s: %s", name, e)
        return None

    # ------------------------------------------------------------------
    # Task submission
    # ------------------------------------------------------------------

    async def execute(
        self,
        *,
        task_id: str,
        step_name: str,
        payload: dict[str, Any],
        timeout_seconds: float = 60.0,
    ) -> dict[str, Any]:
        """Execute task synchronously (wait for result)."""
        worker_h = self._select_worker()
        return await self._call_worker(
            worker_h, "execute", task_id=task_id, step_name=step_name,
            payload=payload, timeout_seconds=timeout_seconds,
        )

    async def submit(
        self,
        *,
        task_id: str,
        step_name: str,
        payload: dict[str, Any],
    ) -> None:
        """Submit task (async mode — returns after submission ACK)."""
        worker_h = self._select_worker()
        await self._call_worker(
            worker_h, "submit", task_id=task_id, step_name=step_name, payload=payload
        )

    async def submit_streaming(
        self,
        *,
        task_id: str,
        step_name: str,
        payload: dict[str, Any],
        buffer_key: str,
    ) -> None:
        """Submit task to streaming path."""
        worker_h = self._select_worker()
        await self._call_worker(
            worker_h, "submit_streaming",
            task_id=task_id, step_name=step_name,
            payload=payload, buffer_key=buffer_key,
        )

    async def _call_worker(
        self,
        worker_h: _WorkerHandle,
        method: str,
        **kwargs: Any,
    ) -> Any:
        """Dispatch a method call to *worker_h* with fault-recovery."""
        try:
            return await self._dispatch(worker_h, method, **kwargs)
        except Exception as exc:
            if _is_actor_fault(exc):
                if worker_h.is_tripped():
                    logger.error(
                        "ActorPoolManager: circuit-breaker tripped for %s", worker_h.name
                    )
                    raise
                worker_h.record_fault()
                logger.warning(
                    "ActorPoolManager: actor fault on %s, replacing and retrying", worker_h.name
                )
                new_actor = await self._replace_worker(worker_h)
                if new_actor is not None:
                    return await self._dispatch(worker_h, method, **kwargs)
            raise

    async def _dispatch(self, worker_h: _WorkerHandle, method: str, **kwargs: Any) -> Any:
        actor = worker_h.actor
        if _RAY_AVAILABLE and hasattr(actor, method):
            m = getattr(actor, method)
            ref = m.remote(**kwargs)
            worker_h.inflight.append(ref)
            try:
                return ray.get(ref, timeout=kwargs.get("timeout_seconds", 60))
            finally:
                worker_h.inflight.discard(ref) if hasattr(worker_h.inflight, "discard") else None
                if ref in worker_h.inflight:
                    worker_h.inflight.remove(ref)
        elif hasattr(actor, method):
            m = getattr(actor, method)
            if asyncio.iscoroutinefunction(m):
                return await m(**kwargs)
            return m(**kwargs)
        raise AttributeError(f"Worker {worker_h.name!r} has no method {method!r}")

    async def _replace_worker(self, worker_h: _WorkerHandle) -> Any | None:
        idx = next(
            (i for i, w in enumerate(self._workers) if w is worker_h), None
        )
        if idx is None:
            return None
        name = self._worker_name(idx)
        new_actor = await self._try_recover_or_create(name, idx)
        if new_actor is not None:
            worker_h.actor = new_actor
            worker_h.inflight.clear()
            worker_h.alive = True
        return new_actor

    # ------------------------------------------------------------------
    # Least-Loaded + Round-Robin selection
    # ------------------------------------------------------------------

    def _refresh_inflight(self, worker_h: _WorkerHandle) -> None:
        """Remove completed ObjectRefs from inflight list."""
        if not _RAY_AVAILABLE:
            return
        done: list[Any] = []
        for ref in worker_h.inflight:
            try:
                ready, _ = ray.wait([ref], timeout=0)
                if ready:
                    done.append(ref)
            except Exception:
                done.append(ref)
        for ref in done:
            worker_h.inflight.remove(ref)

    def _select_worker(self) -> _WorkerHandle:
        if not self._workers:
            raise RuntimeError(
                f"ActorPoolManager: no workers available for capability={self._capability!r}"
            )
        for w in self._workers:
            self._refresh_inflight(w)

        min_pending = min(w.pending for w in self._workers)
        n = len(self._workers)
        for offset in range(n):
            idx = (self._next_index + offset) % n
            if self._workers[idx].pending == min_pending:
                self._next_index = (idx + 1) % n
                return self._workers[idx]
        # Fallback (should not reach)
        return self._workers[self._next_index % n]

    # ------------------------------------------------------------------
    # Dynamic resize
    # ------------------------------------------------------------------

    async def resize(self, new_size: int) -> None:
        """Resize pool to *new_size* (scale-up or scale-down)."""
        new_size = max(0, min(new_size, self._max_size))
        async with self._lock:
            current = len(self._workers)
            if new_size > current:
                for idx in range(current, new_size):
                    name = self._worker_name(idx)
                    actor = await self._try_recover_or_create(name, idx)
                    if actor:
                        self._workers.append(_WorkerHandle(name=name, actor=actor))
                logger.info(
                    "ActorPoolManager resize cap=%s %d→%d",
                    self._capability, current, len(self._workers),
                )
            elif new_size < current:
                # Sort by inflight ascending (remove least-busy first)
                self._workers.sort(key=lambda w: w.pending)
                to_remove = self._workers[new_size:]
                for w in to_remove:
                    if w.pending > 0:
                        logger.warning(
                            "ActorPoolManager: forcibly removing %s with %d inflight tasks",
                            w.name, w.pending,
                        )
                        if _RAY_AVAILABLE:
                            for ref in w.inflight:
                                try:
                                    ray.cancel(ref)
                                except Exception:
                                    pass
                        if _RAY_AVAILABLE:
                            try:
                                ray.kill(w.actor)
                            except Exception:
                                pass
                self._workers = self._workers[:new_size]
                self._next_index = self._next_index % max(1, len(self._workers))
                logger.info(
                    "ActorPoolManager resize cap=%s %d→%d",
                    self._capability, current, len(self._workers),
                )
        self._report_observed_size()

    def _report_observed_size(self) -> None:
        logger.debug(
            "ActorPoolManager observed_size cap=%s size=%d",
            self._capability, len(self._workers),
        )

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    async def get_status(self) -> dict[str, Any]:
        for w in self._workers:
            self._refresh_inflight(w)
        return {
            "capability": self._capability,
            "cluster_id": self._cluster_id,
            "tenant_id": self._tenant_id,
            "pool_size": len(self._workers),
            "total_inflight": sum(w.pending for w in self._workers),
            "workers": [
                {
                    "name": w.name,
                    "pending": w.pending,
                    "alive": w.alive,
                    "circuit_tripped": w.is_tripped(),
                }
                for w in self._workers
            ],
        }
