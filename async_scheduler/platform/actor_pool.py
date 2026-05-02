"""ActorPoolManager: Ray-style Actor Pool abstraction.

Provides a capability-aware actor pool that manages worker actor lifecycle,
least-loaded routing, versioned naming, and health monitoring.

Design mirrors the deepwiki G9 SchedulerActor / ActorPoolManager spec:
- Named actors with versioned keys (actor:{capability}:{version}:{instance_id})
- Least-loaded selection (pending_tasks / max_tasks)
- Automatic eviction of unhealthy / stale actors
- In-process implementation that does NOT require a real Ray runtime;
  actors are modeled as lightweight asyncio coroutine workers.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

__all__ = [
    "ActorState",
    "ActorHandle",
    "ActorPoolConfig",
    "ActorPoolManager",
    "SchedulerActor",
]


class ActorState(str, Enum):
    """Lifecycle state of a single actor instance."""
    INITIALIZING = "initializing"
    HEALTHY = "healthy"
    DRAINING = "draining"   # accepting no new tasks, finishing in-flight
    FAILED = "failed"
    TERMINATED = "terminated"


@dataclass
class ActorHandle:
    """Lightweight handle referencing an actor in the pool."""
    actor_id: str
    capability: str
    version: str
    instance_id: str
    state: ActorState = ActorState.INITIALIZING
    pending_tasks: int = 0
    max_tasks: int = 8
    last_heartbeat_at: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def utilization(self) -> float:
        return self.pending_tasks / max(1, self.max_tasks)

    @property
    def is_available(self) -> bool:
        return (
            self.state == ActorState.HEALTHY
            and self.pending_tasks < self.max_tasks
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "actor_id": self.actor_id,
            "capability": self.capability,
            "version": self.version,
            "instance_id": self.instance_id,
            "state": self.state.value,
            "pending_tasks": self.pending_tasks,
            "max_tasks": self.max_tasks,
            "utilization": round(self.utilization, 3),
            "last_heartbeat_at": self.last_heartbeat_at,
            "metadata": self.metadata,
        }


@dataclass
class ActorPoolConfig:
    """Configuration for an actor pool."""
    min_actors: int = 1
    max_actors: int = 8
    max_tasks_per_actor: int = 8
    idle_timeout_seconds: float = 300.0
    heartbeat_interval_seconds: float = 30.0
    version: str = "v1"


class SchedulerActor:
    """In-process scheduler actor that processes tasks for a specific capability.

    In a real Ray deployment this would be a Ray Detached Named Actor.
    Here we implement it as an asyncio task with a bounded task queue.
    """

    def __init__(
        self,
        capability: str,
        handler: Callable[[dict[str, Any]], Awaitable[Any]],
        max_tasks: int = 8,
        version: str = "v1",
        actor_id: str | None = None,
    ) -> None:
        self.capability = capability
        self.handler = handler
        self.max_tasks = max_tasks
        self.version = version
        self.actor_id = actor_id or str(uuid.uuid4())
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_tasks * 2)
        self._running = False
        self._worker_task: asyncio.Task | None = None
        self._handle = ActorHandle(
            actor_id=self.actor_id,
            capability=capability,
            version=version,
            instance_id=self.actor_id,
            max_tasks=max_tasks,
        )

    @property
    def handle(self) -> ActorHandle:
        return self._handle

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._handle.state = ActorState.HEALTHY
        self._worker_task = asyncio.create_task(self._run_loop())
        logger.info("SchedulerActor started capability=%s actor_id=%s", self.capability, self.actor_id)

    async def stop(self) -> None:
        self._running = False
        self._handle.state = ActorState.DRAINING
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        self._handle.state = ActorState.TERMINATED
        logger.info("SchedulerActor stopped capability=%s actor_id=%s", self.capability, self.actor_id)

    async def submit(self, payload: dict[str, Any]) -> asyncio.Future:
        """Submit a task to this actor. Returns a Future for the result."""
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        try:
            self._queue.put_nowait((payload, fut))
            self._handle.pending_tasks += 1
        except asyncio.QueueFull:
            fut.set_exception(RuntimeError(f"Actor {self.actor_id} queue full"))
        return fut

    async def _run_loop(self) -> None:
        while self._running:
            try:
                payload, fut = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                try:
                    result = await self.handler(payload)
                    if not fut.done():
                        fut.set_result(result)
                except Exception as e:
                    if not fut.done():
                        fut.set_exception(e)
                finally:
                    self._handle.pending_tasks = max(0, self._handle.pending_tasks - 1)
                    self._handle.last_heartbeat_at = time.time()
            except asyncio.TimeoutError:
                self._handle.last_heartbeat_at = time.time()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("SchedulerActor run_loop error: %s", e, exc_info=True)


class ActorPoolManager:
    """Manages a pool of SchedulerActors per capability.

    Provides:
    - Least-loaded actor selection
    - Dynamic scale-up (up to max_actors)
    - Stale actor eviction
    - Pool-level stats
    """

    def __init__(self) -> None:
        self._pools: dict[str, list[SchedulerActor]] = {}
        self._configs: dict[str, ActorPoolConfig] = {}
        self._handlers: dict[str, Callable] = {}
        self._lock = asyncio.Lock()

    def register_capability(
        self,
        capability: str,
        handler: Callable[[dict[str, Any]], Awaitable[Any]],
        config: ActorPoolConfig | None = None,
    ) -> None:
        """Register a capability with its handler and pool config."""
        self._handlers[capability] = handler
        self._configs[capability] = config or ActorPoolConfig()
        if capability not in self._pools:
            self._pools[capability] = []

    async def get_or_create_actor(self, capability: str) -> SchedulerActor | None:
        """Get the least-loaded available actor, creating one if needed."""
        if capability not in self._handlers:
            return None

        async with self._lock:
            pool = self._pools.setdefault(capability, [])
            config = self._configs[capability]

            # Clean up terminated actors
            pool[:] = [a for a in pool if a.handle.state not in (ActorState.TERMINATED, ActorState.FAILED)]

            # Find least-loaded available actor
            available = [a for a in pool if a.handle.is_available]
            if available:
                return min(available, key=lambda a: a.handle.utilization)

            # Scale up if under max
            if len(pool) < config.max_actors:
                actor = SchedulerActor(
                    capability=capability,
                    handler=self._handlers[capability],
                    max_tasks=config.max_tasks_per_actor,
                    version=config.version,
                )
                await actor.start()
                pool.append(actor)
                logger.info("ActorPoolManager: scaled up capability=%s total=%d", capability, len(pool))
                return actor

            # All actors full - return least loaded anyway (will queue)
            if pool:
                return min(pool, key=lambda a: a.handle.utilization)

            return None

    async def submit(self, capability: str, payload: dict[str, Any]) -> Any:
        """Submit a task to the appropriate actor pool."""
        actor = await self.get_or_create_actor(capability)
        if actor is None:
            raise RuntimeError(f"No actor available for capability={capability}")
        fut = await actor.submit(payload)
        return await fut

    async def evict_stale(self, capability: str, max_idle_seconds: float = 300.0) -> int:
        """Stop and remove actors idle beyond max_idle_seconds."""
        async with self._lock:
            pool = self._pools.get(capability, [])
            config = self._configs.get(capability, ActorPoolConfig())
            now = time.time()
            to_evict = []
            for actor in pool:
                idle_secs = now - actor.handle.last_heartbeat_at
                if (
                    actor.handle.pending_tasks == 0
                    and idle_secs > max_idle_seconds
                    and len(pool) - len(to_evict) > config.min_actors
                ):
                    to_evict.append(actor)
            for actor in to_evict:
                await actor.stop()
                pool.remove(actor)
            return len(to_evict)

    async def stop_all(self) -> None:
        """Stop all actors across all capabilities."""
        for pool in self._pools.values():
            for actor in pool:
                await actor.stop()
        self._pools.clear()

    def get_pool_stats(self, capability: str) -> dict[str, Any]:
        """Return stats for a capability pool."""
        pool = self._pools.get(capability, [])
        return {
            "capability": capability,
            "actor_count": len(pool),
            "actors": [a.handle.to_dict() for a in pool],
            "total_pending": sum(a.handle.pending_tasks for a in pool),
            "config": self._configs.get(capability, ActorPoolConfig()).__dict__,
        }

    def list_capabilities(self) -> list[str]:
        return list(self._handlers.keys())
