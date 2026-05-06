"""TaskConsumer — aligned with docs/deepwiki-reference/任务执行.md

Polls capability queues, dequeues tasks, dispatches to executor.
Also handles stale running entry cleanup.
"""
from __future__ import annotations

import asyncio

_LUA_ACQUIRE_SLOT = """
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
local limit = tonumber(ARGV[1])
if current < limit then
    redis.call("INCR", KEYS[1])
    redis.call("EXPIRE", KEYS[1], 300)
    return 1
end
return 0
"""
_LUA_RELEASE_SLOT = """
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
if current > 0 then
    return redis.call("DECR", KEYS[1])
end
return 0
"""
_GLOBAL_CONC_KEY = "global:consumer:concurrency"
import logging
from typing import Any

logger = logging.getLogger(__name__)


class TaskConsumer:
    """Polls queues and drives TaskExecutor."""

    def __init__(
        self,
        queue_manager: Any,
        task_executor: Any,
        capabilities: list[str],
        *,
        poll_interval: float = 1.0,
        max_concurrent: int = 8,
        stale_threshold_seconds: float = 300.0,
        redis_client: Any = None,
    ) -> None:
        self._queue = queue_manager
        self._executor = task_executor
        self._capabilities = capabilities
        self._poll_interval = poll_interval
        self._max_concurrent = max_concurrent
        self._stale_threshold = stale_threshold_seconds
        self._global_conc_limit = max_concurrent
        self._lua_acquire_slot = redis_client.register_script(_LUA_ACQUIRE_SLOT)
        self._lua_release_slot = redis_client.register_script(_LUA_RELEASE_SLOT)
        self._running = False
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._use_global_conc = redis_client is not None
        self._tasks: set[asyncio.Task] = set()

        # Weighted round-robin state
        self._active_caps: dict[str, int] = {}  # cap -> consecutive empty rounds
        self._burst_per_active: int = 3
        self._backoff_multiplier: float = 1.0
        self._max_backoff: float = 8.0
        self._empty_rounds = 0

    async def start(self) -> None:
        self._running = True
        logger.info("TaskConsumer started caps=%s", self._capabilities)
        await self._poll_loop()

    async def stop(self) -> None:
        self._running = False
        for t in list(self._tasks):
            t.cancel()
        
        # Shutdown task executor
        if self._executor is not None and hasattr(self._executor, 'shutdown'):
            try:
                await self._executor.shutdown()
                logger.debug("TaskConsumer: shutdown task executor")
            except Exception as e:
                logger.warning("TaskConsumer: failed to shutdown executor: %s", e)

    async def _poll_loop(self) -> None:
        while self._running:
            got_task = False

            # Phase 1: Burst dequeue from active caps (recently had tasks)
            for cap in list(self._active_caps.keys()):
                for _ in range(self._burst_per_active):
                    if await self._try_dequeue(cap):
                        got_task = True
                        self._active_caps[cap] = 0  # reset empty counter
                    else:
                        # Track consecutive empty rounds for this cap
                        self._active_caps[cap] = self._active_caps.get(cap, 0) + 1
                        if self._active_caps[cap] >= 3:
                            # Remove from active set after 3 empty rounds
                            del self._active_caps[cap]
                        break

            # Phase 2: Normal round-robin for all caps
            for cap in self._capabilities:
                if cap in self._active_caps:
                    continue  # already processed in burst phase
                if await self._try_dequeue(cap):
                    got_task = True
                    self._active_caps[cap] = 0  # add to active set

            # Backoff logic: if no tasks, increase wait time
            if got_task:
                self._backoff_multiplier = 1.0
                self._empty_rounds = 0
            else:
                self._empty_rounds += 1
                self._backoff_multiplier = min(
                    self._max_backoff,
                    self._backoff_multiplier * 2
                )

            wait_time = self._poll_interval * self._backoff_multiplier
            await asyncio.sleep(wait_time)

    async def _try_dequeue(self, capability: str) -> bool:
        """Try to dequeue one task. Returns True if a task was dispatched."""
        # Acquire semaphore first to bound concurrency BEFORE we pop from queue
        # This ensures we never pop a task we cannot execute.
        acquired = False
        try:
            # Non-blocking check: if semaphore is exhausted, skip immediately
            if self._semaphore._value <= 0:
                return False
            # Global concurrency check (P1 fix)
            if self._use_global_conc:
                deadline = asyncio.get_event_loop().time() + 30.0
                while asyncio.get_event_loop().time() < deadline:
                    try:
                        ok = await self._lua_acquire_slot(
                            keys=[_GLOBAL_CONC_KEY],
                            args=[str(self._global_conc_limit)]
                        )
                    except asyncio.CancelledError:
                        raise
                    if ok == 1:
                        break
                    await asyncio.sleep(0.5)
                else:
                    return False
            await self._semaphore.acquire()
            acquired = True

            try:
                task_data = await self._queue.dequeue(capability)
            except Exception as e:
                logger.error("TaskConsumer: dequeue error cap=%s: %s", capability, e)
                if self._use_global_conc:
                    await self._lua_release_slot(keys=[_GLOBAL_CONC_KEY])
                self._semaphore.release()
                return False

            if task_data is None:
                if self._use_global_conc:
                    await self._lua_release_slot(keys=[_GLOBAL_CONC_KEY])
                self._semaphore.release()
                return False

            t = asyncio.create_task(self._execute_task(task_data, capability))
            self._tasks.add(t)
            t.add_done_callback(self._tasks.discard)
            return True
        except asyncio.CancelledError:
            if acquired:
                if self._use_global_conc:
                    await self._lua_release_slot(keys=[_GLOBAL_CONC_KEY])
                self._semaphore.release()
            raise

    async def _execute_task(self, task_data: Any, capability: str) -> None:
        task_id = task_data.get("task_id", "unknown")
        try:
            await self._executor.execute(task_id, None, task_data)
        except Exception as e:
            logger.error("TaskConsumer: task_id=%s failed: %s", task_id, e)
        finally:
            self._semaphore.release()
