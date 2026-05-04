"""Worker registry for distributed scheduler mode.

Architecture
------------
Two separate Redis structures are maintained per worker:

1. **Worker hash**  ``{ns}:worker:{worker_id}``
   Stores serialised ``WorkerInfo``. Persists indefinitely (no TTL).
   Written on register, updated on heartbeat, deleted on deregister.

2. **Liveness key** ``{ns}:worker:{worker_id}:live``
   A plain string (value = "1") with a ``pexpire`` TTL equal to
   ``heartbeat_ttl_seconds``.  Presence == worker is live.
   Renewed on every heartbeat; deleted on deregister.

This separation lets ``list_workers(include_stale=True)`` enumerate
*all* registered workers (via the hash keys) while still reporting
accurate ``is_live`` state (via the liveness key).

Fallback
--------
When no Redis client is available the registry uses an in-process
dict + expiry map (original behaviour, unchanged).
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any

try:
    from redis.asyncio import Redis
except Exception:  # pragma: no cover
    Redis = None


@dataclass
class WorkerInfo:
    worker_id: str
    name: str
    registered_at: datetime = field(default_factory=datetime.utcnow)
    last_heartbeat_at: datetime | None = None
    is_stale: bool = False  # True = worker has timed out / lost heartbeat


class WorkerRegistry:
    """Redis-backed worker liveness registry with stable enumeration.

    See module docstring for the dual-key architecture.
    """

    def __init__(
        self,
        redis_url: str,
        heartbeat_ttl_seconds: float = 30.0,
        namespace: str = "async-scheduler",
        client: Any | None = None,
    ) -> None:
        self._redis_url = redis_url
        self._heartbeat_ttl_seconds = heartbeat_ttl_seconds
        self._namespace = namespace
        self._client = client or (Redis.from_url(redis_url, decode_responses=True) if Redis is not None else None)
        self._client_supports_worker_ops = self._client is not None and all(
            hasattr(self._client, attr) for attr in ("hset", "hgetall", "expire", "exists", "delete", "keys", "set")
        )
        self._guard = asyncio.Lock()
        self._workers: dict[str, WorkerInfo] = {}
        self._expiry: dict[str, datetime] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def register(self, worker: WorkerInfo) -> None:
        if self._client_supports_worker_ops:
            now = datetime.utcnow()
            worker.registered_at = now
            worker.last_heartbeat_at = now
            worker.is_stale = False
            await self._client.hset(self._worker_key(worker.worker_id), mapping=self._serialize_worker(worker))
            await self._set_liveness(worker.worker_id)
            return
        async with self._guard:
            now = datetime.utcnow()
            worker.registered_at = now
            worker.last_heartbeat_at = now
            worker.is_stale = False
            self._workers[worker.worker_id] = worker
            self._expiry[worker.worker_id] = now + timedelta(seconds=self._heartbeat_ttl_seconds)

    async def heartbeat(self, worker_id: str) -> bool:
        if self._client_supports_worker_ops:
            key = self._worker_key(worker_id)
            data = await self._client.hgetall(key)
            if not data:
                return False
            worker = self._deserialize_worker(data)
            worker.last_heartbeat_at = datetime.utcnow()
            worker.is_stale = False
            await self._client.hset(key, mapping=self._serialize_worker(worker))
            await self._set_liveness(worker_id)
            return True
        async with self._guard:
            self._refresh_liveness(worker_id)
            worker = self._workers.get(worker_id)
            if worker is None:
                return False
            now = datetime.utcnow()
            worker.last_heartbeat_at = now
            worker.is_stale = False
            self._expiry[worker_id] = now + timedelta(seconds=self._heartbeat_ttl_seconds)
            return True

    async def is_live(self, worker_id: str) -> bool:
        """Return True if worker is alive (liveness key exists / not stale)."""
        if self._client_supports_worker_ops:
            return bool(await self._client.exists(self._liveness_key(worker_id)))
        async with self._guard:
            self._refresh_liveness(worker_id)
            worker = self._workers.get(worker_id)
            return False if worker is None else not worker.is_stale

    async def is_alive(self, worker_id: str) -> bool:
        """Alias for is_live (doc-compatible name)."""
        return await self.is_live(worker_id)

    async def list_workers(self, include_stale: bool = False) -> list[WorkerInfo]:
        if self._client_supports_worker_ops:
            workers: list[WorkerInfo] = []
            for key in await self._client.keys(f"{self._namespace}:worker:*"):
                # Skip liveness keys (they end with :live)
                if key.endswith(":live"):
                    continue
                data = await self._client.hgetall(key)
                if not data:
                    continue
                worker = self._deserialize_worker(data)
                # Derive is_live from the presence of the separate liveness key
                worker.is_stale = not bool(await self._client.exists(self._liveness_key(worker.worker_id)))
                workers.append(worker)
            if not include_stale:
                workers = [w for w in workers if not w.is_stale]
            return sorted(workers, key=lambda w: w.worker_id)
        async with self._guard:
            for worker_id in list(self._workers.keys()):
                self._refresh_liveness(worker_id)
            workers = list(self._workers.values())
            if not include_stale:
                workers = [w for w in workers if not w.is_stale]
            return sorted(workers, key=lambda w: w.worker_id)

    async def deregister(self, worker_id: str) -> bool:
        if self._client_supports_worker_ops:
            deleted = await self._client.delete(self._worker_key(worker_id))
            await self._client.delete(self._liveness_key(worker_id))
            return bool(deleted)
        async with self._guard:
            existed = worker_id in self._workers
            self._workers.pop(worker_id, None)
            self._expiry.pop(worker_id, None)
            return existed

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _set_liveness(self, worker_id: str) -> None:
        """Write or renew the liveness key with the heartbeat TTL."""
        ttl_ms = max(100, int(self._heartbeat_ttl_seconds * 1000))
        lkey = self._liveness_key(worker_id)
        await self._client.set(lkey, "1")
        await self._client.pexpire(lkey, ttl_ms)

    def _refresh_liveness(self, worker_id: str) -> None:
        worker = self._workers.get(worker_id)
        expires_at = self._expiry.get(worker_id)
        if worker is None or expires_at is None:
            return
        worker.is_stale = expires_at <= datetime.utcnow()

    def _worker_key(self, worker_id: str) -> str:
        return f"{self._namespace}:worker:{worker_id}"

    def _liveness_key(self, worker_id: str) -> str:
        return f"{self._namespace}:worker:{worker_id}:live"

    def _serialize_worker(self, worker: WorkerInfo) -> dict[str, str]:
        return {
            "worker_id": worker.worker_id,
            "name": worker.name,
            "registered_at": worker.registered_at.isoformat(),
            "last_heartbeat_at": "" if worker.last_heartbeat_at is None else worker.last_heartbeat_at.isoformat(),
            "is_stale": "1" if worker.is_stale else "0",
        }

    def _deserialize_worker(self, data: dict[str, str]) -> WorkerInfo:
        return WorkerInfo(
            worker_id=data["worker_id"],
            name=data["name"],
            registered_at=datetime.fromisoformat(data["registered_at"]),
            last_heartbeat_at=None
            if not data.get("last_heartbeat_at")
            else datetime.fromisoformat(data["last_heartbeat_at"]),
            is_stale=data.get("is_stale", "0") == "1",
        )
