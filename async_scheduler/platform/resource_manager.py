"""ResourceManager: queue-depth-driven scale-up/down decisions.

Implements G10 from the deepwiki P2 roadmap:
- Monitors per-capability queue depths and running counts
- Emits scale-up / scale-down signals when utilization crosses thresholds
- Integrates with ActorPoolManager to actually resize pools
- Designed to run as a periodic background task

Scale policy:
  - scale_up   when pending > running * scale_up_threshold
  - scale_down when pending == 0 AND running == 0 for scale_down_idle_seconds
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from async_scheduler.platform.actor_pool import ActorPoolManager
    from async_scheduler.queue.manager import QueueManager

logger = logging.getLogger(__name__)

__all__ = ["ResourcePolicy", "ScaleEvent", "ScaleDirection", "ResourceManager"]


class ScaleDirection(str):
    UP = "up"
    DOWN = "down"
    NONE = "none"


@dataclass
class ScaleEvent:
    """Emitted when a scale decision is made."""
    capability: str
    direction: str  # "up" | "down" | "none"
    current_actors: int
    target_actors: int
    reason: str
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "capability": self.capability,
            "direction": self.direction,
            "current_actors": self.current_actors,
            "target_actors": self.target_actors,
            "reason": self.reason,
            "timestamp": self.timestamp,
        }


@dataclass
class ResourcePolicy:
    """Per-capability or global resource scaling policy."""
    min_actors: int = 1
    max_actors: int = 8
    # Scale up when pending_tasks > running_count * this factor
    scale_up_threshold: float = 1.5
    # Scale down when idle for this many seconds
    scale_down_idle_seconds: float = 120.0
    # How many actors to add/remove per scale event
    scale_step: int = 1
    # Check interval in seconds
    check_interval_seconds: float = 30.0


class ResourceManager:
    """Monitors queue depths and drives actor pool scaling decisions.

    Usage::

        rm = ResourceManager(queue_manager=qm, actor_pool=apm)
        rm.register_policy("gpu", ResourcePolicy(min_actors=2, max_actors=16))
        await rm.start()
        # Runs background loop; stop with await rm.stop()
    """

    def __init__(
        self,
        queue_manager: "QueueManager",
        actor_pool: "ActorPoolManager | None" = None,
        default_policy: ResourcePolicy | None = None,
    ) -> None:
        self._queue_manager = queue_manager
        self._actor_pool = actor_pool
        self._default_policy = default_policy or ResourcePolicy()
        self._policies: dict[str, ResourcePolicy] = {}
        self._idle_since: dict[str, float] = {}
        self._scale_history: list[ScaleEvent] = []
        self._running = False
        self._task: asyncio.Task | None = None

    def register_policy(self, capability: str, policy: ResourcePolicy) -> None:
        self._policies[capability] = policy

    def _get_policy(self, capability: str) -> ResourcePolicy:
        return self._policies.get(capability, self._default_policy)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info("ResourceManager started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("ResourceManager stopped")

    async def _monitor_loop(self) -> None:
        while self._running:
            try:
                await self._check_all_capabilities()
            except Exception as e:
                logger.error("ResourceManager monitor error: %s", e, exc_info=True)
            # Use shortest check interval across all policies
            intervals = [p.check_interval_seconds for p in self._policies.values()]
            interval = min(intervals) if intervals else self._default_policy.check_interval_seconds
            await asyncio.sleep(interval)

    async def _check_all_capabilities(self) -> None:
        capabilities = await self._queue_manager.discover_capabilities()
        for cap in capabilities:
            try:
                await self._check_capability(cap)
            except Exception as e:
                logger.warning("ResourceManager capability check failed cap=%s: %s", cap, e)

    async def _check_capability(self, capability: str) -> None:
        policy = self._get_policy(capability)

        # Get queue depths
        stats = await self._queue_manager.get_capability_stats(capability)
        pending = stats.pending
        running = stats.running

        # Determine current actor count
        current_actors = 1
        if self._actor_pool is not None:
            pool_stats = self._actor_pool.get_pool_stats(capability)
            current_actors = pool_stats["actor_count"]

        now = time.time()

        # Scale-up logic
        if pending > running * policy.scale_up_threshold:
            target = min(current_actors + policy.scale_step, policy.max_actors)
            if target > current_actors:
                event = ScaleEvent(
                    capability=capability,
                    direction=ScaleDirection.UP,
                    current_actors=current_actors,
                    target_actors=target,
                    reason=f"pending={pending} > running={running} * threshold={policy.scale_up_threshold}",
                )
                await self._apply_scale(capability, event)
                self._idle_since.pop(capability, None)
                return

        # Scale-down logic
        if pending == 0 and running == 0:
            if capability not in self._idle_since:
                self._idle_since[capability] = now
            idle_duration = now - self._idle_since[capability]
            if idle_duration >= policy.scale_down_idle_seconds:
                target = max(current_actors - policy.scale_step, policy.min_actors)
                if target < current_actors:
                    event = ScaleEvent(
                        capability=capability,
                        direction=ScaleDirection.DOWN,
                        current_actors=current_actors,
                        target_actors=target,
                        reason=f"idle for {idle_duration:.0f}s",
                    )
                    await self._apply_scale(capability, event)
        else:
            self._idle_since.pop(capability, None)

    async def _apply_scale(self, capability: str, event: ScaleEvent) -> None:
        self._scale_history.append(event)
        if len(self._scale_history) > 500:
            self._scale_history = self._scale_history[-500:]

        logger.info(
            "ResourceManager scale %s capability=%s %d→%d reason=%s",
            event.direction, capability, event.current_actors, event.target_actors, event.reason,
        )

        if self._actor_pool is None:
            return

        if event.direction == ScaleDirection.UP:
            # Trigger actor pool to create more actors by submitting a probe
            delta = event.target_actors - event.current_actors
            for _ in range(delta):
                await self._actor_pool.get_or_create_actor(capability)

        elif event.direction == ScaleDirection.DOWN:
            delta = event.current_actors - event.target_actors
            policy = self._get_policy(capability)
            await self._actor_pool.evict_stale(capability, max_idle_seconds=policy.scale_down_idle_seconds)

    def get_scale_history(self, limit: int = 50) -> list[dict[str, Any]]:
        return [e.to_dict() for e in self._scale_history[-limit:]]

    def get_stats(self) -> dict[str, Any]:
        return {
            "running": self._running,
            "tracked_capabilities": list(self._policies.keys()),
            "scale_events_total": len(self._scale_history),
            "recent_events": self.get_scale_history(10),
        }
