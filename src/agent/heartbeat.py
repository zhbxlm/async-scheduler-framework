"""Agent heartbeat + ownership protocol.

Ownership key: agent_owner:{node_id}  (Redis SET NX EX)
Lua CAS for renewal and release to prevent concurrent agent races.
"""
from __future__ import annotations
import asyncio
import logging
import uuid
from typing import Any

logger = logging.getLogger(__name__)

_LUA_RENEW = """
local key = KEYS[1]
local val = ARGV[1]
local ttl = tonumber(ARGV[2])
if redis.call('GET', key) == val then
    redis.call('EXPIRE', key, ttl)
    return 'ok'
end
return 'lost'
"""

_LUA_RELEASE = """
local key = KEYS[1]
local val = ARGV[1]
if redis.call('GET', key) == val then
    redis.call('DEL', key)
    return 'ok'
end
return 'not_owner'
"""

_OWNER_KEY = "agent_owner:{node_id}"
_MAX_RETRY = 3


class HeartbeatProtocol:
    """Manages agent ownership and NodeRegistry heartbeat."""

    def __init__(
        self,
        redis_client: Any,
        node_registry: Any,
        node_id: str,
        *,
        heartbeat_interval: float = 15.0,
        owner_ttl: int = 90,
        instance_id: str | None = None,
    ) -> None:
        self._r = redis_client
        self._nodes = node_registry
        self._node_id = node_id
        self._interval = heartbeat_interval
        self._owner_ttl = owner_ttl
        self._instance_id = instance_id or str(uuid.uuid4())
        self._owner_key = _OWNER_KEY.format(node_id=node_id)
        self._running = False
        self._is_owner = False

    async def acquire_ownership(self) -> bool:
        ok = await self._r.set(self._owner_key, self._instance_id, nx=True, ex=self._owner_ttl)
        if ok:
            self._is_owner = True
            logger.info("HeartbeatProtocol: acquired ownership node=%s", self._node_id)
        return bool(ok)

    async def release_ownership(self) -> None:
        result = await self._r.eval(_LUA_RELEASE, 1, self._owner_key, self._instance_id)
        self._is_owner = False
        logger.info("HeartbeatProtocol: released ownership node=%s result=%s", self._node_id, result)

    async def start(self) -> None:
        self._running = True
        await self._heartbeat_loop()

    async def stop(self) -> None:
        self._running = False

    @property
    def is_owner(self) -> bool:
        return self._is_owner

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._interval)
            success = False
            for attempt in range(_MAX_RETRY):
                try:
                    result = await self._r.eval(
                        _LUA_RENEW, 1, self._owner_key,
                        self._instance_id, str(self._owner_ttl),
                    )
                    status = result.decode() if isinstance(result, bytes) else result
                    if status == "ok":
                        # Also beat NodeRegistry
                        await self._nodes.heartbeat(self._node_id)
                        success = True
                        break
                    else:
                        # Ownership lost — try re-acquire
                        re_ok = await self.acquire_ownership()
                        if re_ok:
                            success = True
                            break
                except Exception as e:
                    logger.warning("HeartbeatProtocol: attempt %d failed: %s", attempt, e)
                    await asyncio.sleep(2 ** attempt)

            if not success:
                logger.error("HeartbeatProtocol: all retries exhausted, stopping node=%s", self._node_id)
                self._is_owner = False
                self._running = False
                return
