"""TaskExecutor — aligned with docs/deepwiki-reference/任务执行.md

Service layer for task execution:
- Acquire distributed execution lock (Redis SET NX + PX)
- Background lock renewal (heartbeat)
- Drive DagEngine
- Detect cancellation + lock-loss
- Trigger completion callback
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

logger = logging.getLogger(__name__)

_LOCK_KEY = "task_lock:{task_id}"
_CANCEL_KEY = "task_cancel:{task_id}"
_LOCK_RENEWAL_INTERVAL = 10  # seconds
_DEFAULT_LOCK_TTL_MS = 30_000


_LUA_LOCK_RELEASE = """
-- Atomic lock release: only delete if value matches
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""
_LUA_LOCK_RENEW = """
local key = KEYS[1]
local val = ARGV[1]
local ttl = tonumber(ARGV[2])
if redis.call('GET', key) == val then
    redis.call('PEXPIRE', key, ttl)
    return 'ok'
end
return 'lost'
"""


class TaskExecutor:
    """Execute a single task with distributed lock + DAG engine."""

    def __init__(
        self,
        redis_client: Any,
        dag_engine: Any | None = None,
        callback_fn: Any | None = None,
        *,
        lock_ttl_ms: int = _DEFAULT_LOCK_TTL_MS,
    ) -> None:
        self._r = redis_client
        self._dag = dag_engine
        self._callback = callback_fn
        self._lock_ttl_ms = lock_ttl_ms
        self._lua_lock_renew = redis_client.register_script(_LUA_LOCK_RENEW)
        self._lua_lock_release = redis_client.register_script(_LUA_LOCK_RELEASE)
        self._draining = False
        self._active_tasks: set[str] = set()

    async def execute(self, task_id: str, dag_definition: Any, context: Any) -> dict[str, Any]:
        """Acquire lock, run DAG, release lock, callback."""
        lock_val = str(uuid.uuid4())
        lock_key = _LOCK_KEY.format(task_id=task_id)

        # Acquire
        acquired = await self._r.set(lock_key, lock_val, nx=True, px=self._lock_ttl_ms)
        if not acquired:
            raise RuntimeError(f"TaskExecutor: failed to acquire lock for task_id={task_id}")

        self._active_tasks.add(task_id)
        renewal_task = asyncio.create_task(
            self._renew_lock_loop(lock_key, lock_val)
        )
        result = {"status": "unknown"}
        try:
            # Check cancellation
            cancel_key = _CANCEL_KEY.format(task_id=task_id)
            cancelled = await self._r.get(cancel_key)
            if cancelled:
                result = {"status": "cancelled"}
                return result

            # Run DAG
            if self._dag is not None:
                dag_result = await self._dag.execute(dag_definition, context)
                result = {"status": "completed", "dag_result": dag_result}
            else:
                result = {"status": "completed"}

        except asyncio.CancelledError:
            result = {"status": "cancelled"}
            raise
        except Exception as e:
            result = {"status": "failed", "error": str(e)}
            logger.error("TaskExecutor: task_id=%s failed: %s", task_id, e)
        finally:
            self._active_tasks.discard(task_id)
            renewal_task.cancel()
            try:
                await renewal_task
            except asyncio.CancelledError:
                pass
            # Release lock (CAS)
            await self._release_lock(lock_key, lock_val)
            # Callback
            if self._callback:
                try:
                    await self._callback(task_id, result)
                except Exception:
                    pass

        return result

    async def _renew_lock_loop(self, lock_key: str, lock_val: str) -> None:
        while True:
            await asyncio.sleep(_LOCK_RENEWAL_INTERVAL)
            result = await self._lua_lock_renew(
                keys=[lock_key], args=[lock_val, str(self._lock_ttl_ms)]
            )
            status = result.decode() if isinstance(result, bytes) else str(result)
            if status == "lost":
                logger.warning("TaskExecutor: lock lost for key=%s", lock_key)
                break

    async def shutdown(self, timeout: float = 30.0) -> None:
        self._draining = True
        if self._active_tasks:
            deadline = asyncio.get_running_loop().time() + timeout
            while self._active_tasks and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.5)

        # Close DagEngine if provided
        if self._dag is not None and hasattr(self._dag, "close"):
            try:
                await self._dag.close()
                logger.debug("TaskExecutor: closed DagEngine")
            except Exception as e:
                logger.warning("TaskExecutor: failed to close DagEngine: %s", e)

        logger.info("TaskExecutor: drain complete")

    async def _release_lock(self, lock_key: str, lock_val: str) -> None:
        """Atomic lock release via Lua script (P0 fix)."""
        try:
            await self._lua_lock_release(keys=[lock_key], args=[lock_val])
        except Exception:
            pass
