"""Worker registry for distributed scheduler mode."""

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
    capabilities: list[str] = field(default_factory=list)
    registered_at: datetime = field(default_factory=datetime.utcnow)
    last_heartbeat_at: datetime | None = None
    is_live: bool = True


class WorkerRegistry:
    """Redis-shaped worker liveness registry.

    Supports a real Redis client path when available, while preserving the
    original process-local fallback semantics for deterministic tests.
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
            hasattr(self._client, attr) for attr in ("hset", "hgetall", "expire", "exists", "delete", "keys")
        )
        self._guard = asyncio.Lock()
        self._workers: dict[str, WorkerInfo] = {}
        self._expiry: dict[str, datetime] = {}

    async def register(self, worker: WorkerInfo) -> None:
        if self._client_supports_worker_ops:
            now = datetime.utcnow()
            worker.registered_at = now
            worker.last_heartbeat_at = now
            worker.is_live = True
            await self._client.hset(self._worker_key(worker.worker_id), mapping=self._serialize_worker(worker))
            await self._client.expire(self._worker_key(worker.worker_id), int(self._heartbeat_ttl_seconds))
            return
        async with self._guard:
            now = datetime.utcnow()
            worker.registered_at = now
            worker.last_heartbeat_at = now
            worker.is_live = True
            self._workers[worker.worker_id] = worker
            self._expiry[worker.worker_id] = now + timedelta(seconds=self._heartbeat_ttl_seconds)

    async def heartbeat(self, worker_id: str) -> bool:
        if self._client_supports_worker_ops:
            key = self._worker_key(worker_id)
            if not await self._client.exists(key):
                return False
            data = await self._client.hgetall(key)
            worker = self._deserialize_worker(data)
            worker.last_heartbeat_at = datetime.utcnow()
            worker.is_live = True
            await self._client.hset(key, mapping=self._serialize_worker(worker))
            await self._client.expire(key, int(self._heartbeat_ttl_seconds))
            return True
        async with self._guard:
            self._refresh_liveness(worker_id)
            worker = self._workers.get(worker_id)
            if worker is None:
                return False
            now = datetime.utcnow()
            worker.last_heartbeat_at = now
            worker.is_live = True
            self._expiry[worker_id] = now + timedelta(seconds=self._heartbeat_ttl_seconds)
            return True

    async def is_live(self, worker_id: str) -> bool:
        if self._client_supports_worker_ops:
            return bool(await self._client.exists(self._worker_key(worker_id)))
        async with self._guard:
            self._refresh_liveness(worker_id)
            worker = self._workers.get(worker_id)
            return False if worker is None else worker.is_live

    async def list_workers(self, include_stale: bool = False) -> list[WorkerInfo]:
        if self._client_supports_worker_ops:
            workers: list[WorkerInfo] = []
            for key in await self._client.keys(f"{self._namespace}:worker:*"):
                data = await self._client.hgetall(key)
                if not data:
                    continue
                worker = self._deserialize_worker(data)
                worker.is_live = True
                workers.append(worker)
            if not include_stale:
                workers = [worker for worker in workers if worker.is_live]
            return sorted(workers, key=lambda worker: worker.worker_id)
        async with self._guard:
            for worker_id in list(self._workers.keys()):
                self._refresh_liveness(worker_id)
            workers = list(self._workers.values())
            if not include_stale:
                workers = [worker for worker in workers if worker.is_live]
            return sorted(workers, key=lambda worker: worker.worker_id)

    async def deregister(self, worker_id: str) -> bool:
        if self._client_supports_worker_ops:
            deleted = await self._client.delete(self._worker_key(worker_id))
            return bool(deleted)
        async with self._guard:
            existed = worker_id in self._workers
            self._workers.pop(worker_id, None)
            self._expiry.pop(worker_id, None)
            return existed

    def _refresh_liveness(self, worker_id: str) -> None:
        worker = self._workers.get(worker_id)
        expires_at = self._expiry.get(worker_id)
        if worker is None or expires_at is None:
            return
        worker.is_live = expires_at > datetime.utcnow()

    def _worker_key(self, worker_id: str) -> str:
        return f"{self._namespace}:worker:{worker_id}"

    def _serialize_worker(self, worker: WorkerInfo) -> dict[str, str]:
        data = asdict(worker)
        return {
            "worker_id": data["worker_id"],
            "name": data["name"],
            "capabilities": json.dumps(data["capabilities"]),
            "registered_at": worker.registered_at.isoformat(),
            "last_heartbeat_at": "" if worker.last_heartbeat_at is None else worker.last_heartbeat_at.isoformat(),
            "is_live": "1" if worker.is_live else "0",
        }

    def _deserialize_worker(self, data: dict[str, str]) -> WorkerInfo:
        return WorkerInfo(
            worker_id=data["worker_id"],
            name=data["name"],
            capabilities=json.loads(data.get("capabilities", "[]")),
            registered_at=datetime.fromisoformat(data["registered_at"]),
            last_heartbeat_at=None
            if not data.get("last_heartbeat_at")
            else datetime.fromisoformat(data["last_heartbeat_at"]),
            is_live=data.get("is_live", "1") == "1",
        )
