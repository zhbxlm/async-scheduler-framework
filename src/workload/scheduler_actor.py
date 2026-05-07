"""SchedulerActor — aligned with docs/deepwiki-reference/调度与资源管理.md

Ray Detached Named Actor that acts as the cluster-level scheduling entry
point.  Manages per-capability ActorPools, provides three dispatch modes
(execute_sync / submit_async / submit_streaming), and recovers pool state
from Redis after a restart.

In environments without a real Ray runtime, the module gracefully degrades:
 - @ray.remote decorator is a no-op when Ray is not installed
 - get_scheduler_actor() returns a local Python instance instead
This allows unit-testing without a Ray cluster.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ray import with graceful fallback
# ---------------------------------------------------------------------------
try:
    import ray  # type: ignore[import]
    _RAY_AVAILABLE = True
except ImportError:
    ray = None  # type: ignore[assignment]
    _RAY_AVAILABLE = False


def _ray_remote(**kwargs):
    """Decorator factory that wraps @ray.remote or is a no-op."""
    if _RAY_AVAILABLE:
        return ray.remote(**kwargs)
    def _noop(cls_or_fn):
        return cls_or_fn
    return _noop


# ---------------------------------------------------------------------------
# Redis key helpers
# ---------------------------------------------------------------------------
_ACTIVE_KEY = "scheduler:active:{cluster_id}"
_POOL_MAP_KEY = "scheduler:{name}:pools"      # hash capability → pool_name


# ---------------------------------------------------------------------------
# Actor name utilities
# ---------------------------------------------------------------------------

def actor_name(cluster_id: str, release: str) -> str:
    """Return versioned SchedulerActor name."""
    return f"scheduler_{cluster_id}__{release}"


def tenant_actor_namespace(tenant_id: str) -> str:
    """Return Ray actor namespace for a tenant."""
    if not tenant_id or tenant_id in ("default", ""):
        return "ray_async"
    return f"ray_async_{tenant_id}"


def get_scheduler_actor(
    cluster_id: str,
    release: str,
    redis_client: Any = None,
    *,
    tenant_id: str = "",
    auto_create: bool = True,
) -> "SchedulerActor":
    """Locate or create a SchedulerActor for *cluster_id*.

    Priority:
    1. Active version stored in Redis
    2. Requested *release* version
    3. Auto-create if ``auto_create=True``
    """
    namespace = tenant_actor_namespace(tenant_id)
    name = actor_name(cluster_id, release)

    if _RAY_AVAILABLE:
        # Try active version
        if redis_client is not None:
            import asyncio
            try:
                # Use asyncio.run() if no running loop (sync factory context),
                # otherwise fall back to executor to avoid blocking the event loop.
                try:
                    loop = asyncio.get_running_loop()
                    # Already inside an async context — run in executor to avoid deadlock
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                        active_name_raw = pool.submit(
                            asyncio.run,
                            redis_client.get(_ACTIVE_KEY.format(cluster_id=cluster_id))
                        ).result(timeout=2.0)
                except RuntimeError:
                    # No running loop — safe to call asyncio.run() directly
                    active_name_raw = asyncio.run(
                        redis_client.get(_ACTIVE_KEY.format(cluster_id=cluster_id))
                    )
                if active_name_raw:
                    active_name = active_name_raw.decode() if isinstance(active_name_raw, bytes) else active_name_raw
                    try:
                        return ray.get_actor(active_name, namespace=namespace)
                    except Exception:
                        pass
            except Exception:
                pass

        # Try requested version
        try:
            return ray.get_actor(name, namespace=namespace)
        except Exception:
            pass

        if auto_create:
            actor = SchedulerActor.options(  # type: ignore[attr-defined]
                name=name, namespace=namespace, lifetime="detached"
            ).remote(cluster_id=cluster_id, release=release)
            return actor

        raise RuntimeError(f"SchedulerActor not found: cluster={cluster_id} release={release}")

    # Fallback: local instance (test / no-Ray env)
    return SchedulerActor(cluster_id=cluster_id, release=release, redis_client=redis_client)


# ---------------------------------------------------------------------------
# SchedulerActor
# ---------------------------------------------------------------------------

@_ray_remote(num_cpus=0.1)
class SchedulerActor:
    """Cluster-level scheduling proxy managing per-capability ActorPools.

    When running under Ray, this is a Detached Named Actor.
    When Ray is not available, it behaves as a plain Python object.
    """

    def __init__(
        self,
        *,
        cluster_id: str,
        release: str = "v1",
        redis_client: Any = None,
        tenant_id: str = "",
    ) -> None:
        self._cluster_id = cluster_id
        self._release = release
        self._redis = redis_client
        self._tenant_id = tenant_id
        self._name = actor_name(cluster_id, release)
        # capability → ActorPoolManager
        self._pools: dict[str, Any] = {}
        logger.info(
            "SchedulerActor init cluster=%s release=%s ray=%s",
            cluster_id, release, _RAY_AVAILABLE,
        )

    # ------------------------------------------------------------------
    # Pool registration
    # ------------------------------------------------------------------

    async def register_pool(self, capability: str, pool_manager: Any) -> None:
        """Register an ActorPoolManager for *capability*."""
        self._pools[capability] = pool_manager
        if self._redis is not None:
            key = _POOL_MAP_KEY.format(name=self._name)
            await self._redis.hset(key, capability, pool_manager.__class__.__name__)
        logger.info("SchedulerActor.register_pool cap=%s", capability)

    async def _recover_pools_from_redis(self) -> None:
        """Re-attach to surviving ActorPoolManagers after a restart."""
        if self._redis is None or not _RAY_AVAILABLE:
            return
        key = _POOL_MAP_KEY.format(name=self._name)
        mapping = await self._redis.hgetall(key)
        for cap_b, pool_name_b in mapping.items():
            cap = cap_b.decode() if isinstance(cap_b, bytes) else cap_b
            if cap in self._pools:
                continue
            try:
                handle = ray.get_actor(
                    pool_name_b.decode() if isinstance(pool_name_b, bytes) else pool_name_b
                )
                self._pools[cap] = handle
                logger.info("SchedulerActor recovered pool cap=%s", cap)
            except Exception:
                await self._redis.hdel(key, cap)
                logger.warning("SchedulerActor: stale pool entry removed cap=%s", cap)

    # ------------------------------------------------------------------
    # Dispatch modes
    # ------------------------------------------------------------------

    async def execute_sync(
        self,
        capability: str,
        task_id: str,
        step_name: str,
        payload: dict[str, Any],
        *,
        timeout_seconds: float = 60.0,
    ) -> dict[str, Any]:
        """Dispatch to pool and wait for result (synchronous mode)."""
        pool = self._get_pool(capability)
        if _RAY_AVAILABLE:
            ref = pool.execute.remote(task_id=task_id, step_name=step_name, payload=payload)
            result = ray.get(ref, timeout=timeout_seconds)
        else:
            result = await pool.execute(task_id=task_id, step_name=step_name, payload=payload)
        return result

    async def submit_async(
        self,
        capability: str,
        task_id: str,
        step_name: str,
        payload: dict[str, Any],
    ) -> str:
        """Dispatch to pool; wait for submission ACK, return task handle.

        Returns a handle string ``{task_id}:{step_name}``.
        Explicitly waits for submission confirm (not fire-and-forget).
        """
        pool = self._get_pool(capability)
        if _RAY_AVAILABLE:
            ack_ref = pool.submit.remote(task_id=task_id, step_name=step_name, payload=payload)
            ray.get(ack_ref, timeout=10.0)   # wait for submit ACK
        else:
            await pool.submit(task_id=task_id, step_name=step_name, payload=payload)
        return f"{task_id}:{step_name}"

    async def submit_streaming(
        self,
        capability: str,
        task_id: str,
        step_name: str,
        payload: dict[str, Any],
    ) -> str:
        """Dispatch to pool's streaming path; return buffer key."""
        pool = self._get_pool(capability)
        buffer_key = f"stream:{task_id}:{step_name}"
        if _RAY_AVAILABLE:
            ack_ref = pool.submit_streaming.remote(
                task_id=task_id, step_name=step_name,
                payload=payload, buffer_key=buffer_key,
            )
            ray.get(ack_ref, timeout=10.0)
        else:
            if hasattr(pool, "submit_streaming"):
                await pool.submit_streaming(
                    task_id=task_id, step_name=step_name,
                    payload=payload, buffer_key=buffer_key,
                )
        return buffer_key

    # ------------------------------------------------------------------
    # Status / housekeeping
    # ------------------------------------------------------------------

    def get_capabilities(self) -> list[str]:
        return list(self._pools.keys())

    def get_pool_status(self, capability: str) -> dict[str, Any]:
        pool = self._pools.get(capability)
        if pool is None:
            return {"capability": capability, "registered": False}
        if hasattr(pool, "get_status"):
            import asyncio
            try:
                try:
                    asyncio.get_running_loop()
                    # Already in async context: cannot call run_until_complete;
                    # return a pending marker and let caller await asynchronously.
                    return {"capability": capability, "registered": True, "status": "pending_async"}
                except RuntimeError:
                    return asyncio.run(pool.get_status())
            except Exception:
                pass
        return {"capability": capability, "registered": True}

    def _get_pool(self, capability: str) -> Any:
        pool = self._pools.get(capability)
        if pool is None:
            raise RuntimeError(
                f"SchedulerActor: no pool registered for capability={capability!r}"
            )
        return pool
