"""Node Agent — src/agent/server.py
aligned with docs/deepwiki-reference/节点代理.md

Lightweight HTTP service running on each cluster machine.
Handles: ownership protocol, heartbeat, resource detection,
Ray cluster join/leave, deployment package management.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import threading
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.models.node import (
    DrainRequest,
    InvitationRequest,
    InvitationResponse,
    NodeInfo,
    NodeResources,
    NodeState,
    ReleaseRequest,
)
from src.models.deploy import (
    DeployRequest,
    DeployResult,
    DeployState,
    DeployedPackageInfo,
    UndeployRequest,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ownership protocol Redis keys
# ---------------------------------------------------------------------------
_OWNER_KEY = "agent_owner:{node_id}"
_OWNER_TTL_SECONDS = 30
_HEARTBEAT_INTERVAL = 10


# ---------------------------------------------------------------------------
# AgentState
# ---------------------------------------------------------------------------

class AgentState:
    """Thread-safe mutable state container for the node agent."""

    def __init__(self, node_id: str, host: str, agent_port: int = 9100) -> None:
        self.node_id = node_id
        self.host = host
        self.agent_port = agent_port
        self.instance_id = str(uuid.uuid4())
        self.state: NodeState = NodeState.IDLE
        self.cluster_id: str = ""
        self.ray_head_address: str = ""
        self.resources: NodeResources = NodeResources()
        self.deployed_packages: dict[str, DeployedPackageInfo] = {}
        self._lock = threading.Lock()

    def transition(self, new_state: NodeState, **kwargs: Any) -> None:
        with self._lock:
            self.state = new_state
            for k, v in kwargs.items():
                setattr(self, k, v)

    def to_node_info(self) -> NodeInfo:
        with self._lock:
            return NodeInfo(
                node_id=self.node_id,
                host=self.host,
                agent_port=self.agent_port,
                state=self.state,
                resources=self.resources,
                cluster_id=self.cluster_id,
                ray_head_address=self.ray_head_address,
                last_heartbeat=time.time(),
            )


# ---------------------------------------------------------------------------
# Ownership protocol
# ---------------------------------------------------------------------------

_LUA_RENEW = """
local key = KEYS[1]
local id  = ARGV[1]
local ttl = tonumber(ARGV[2])
local cur = redis.call('GET', key)
if cur == id then
    redis.call('EXPIRE', key, ttl)
    return 'ok'
end
return 'mismatch'
"""

_LUA_RELEASE = """
local key = KEYS[1]
local id  = ARGV[1]
local cur = redis.call('GET', key)
if cur == id then
    redis.call('DEL', key)
    return 'ok'
end
return 'mismatch'
"""


async def acquire_ownership(redis_client: Any, node_id: str, instance_id: str) -> bool:
    key = _OWNER_KEY.format(node_id=node_id)
    result = await redis_client.set(key, instance_id, nx=True, ex=_OWNER_TTL_SECONDS)
    return bool(result)


async def renew_ownership(redis_client: Any, node_id: str, instance_id: str) -> bool:
    key = _OWNER_KEY.format(node_id=node_id)
    result = await redis_client.eval(_LUA_RENEW, 1, key, instance_id, str(_OWNER_TTL_SECONDS))
    return result in (b"ok", "ok")


async def release_ownership(redis_client: Any, node_id: str, instance_id: str) -> bool:
    key = _OWNER_KEY.format(node_id=node_id)
    result = await redis_client.eval(_LUA_RELEASE, 1, key, instance_id)
    return result in (b"ok", "ok")


# ---------------------------------------------------------------------------
# Ray process manager
# ---------------------------------------------------------------------------

def ray_start(head_address: str, custom_resources: dict[str, float] | None = None) -> bool:
    """Start Ray worker and join cluster. Idempotent."""
    if _ray_is_running():
        logger.info("ray_start: Ray already running, skip")
        return True
    cmd = ["ray", "start", f"--address={head_address}"]
    if custom_resources:
        res_str = json.dumps(custom_resources)
        cmd += [f"--resources={res_str}"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            logger.error("ray_start failed: %s", result.stderr)
            return False
        # Verify
        verify = subprocess.run(["ray", "status"], capture_output=True, text=True, timeout=10)
        return verify.returncode == 0
    except Exception as e:
        logger.error("ray_start exception: %s", e)
        return False


def ray_stop() -> bool:
    """Stop local Ray worker. Idempotent."""
    if not _ray_is_running():
        return True
    try:
        result = subprocess.run(["ray", "stop"], capture_output=True, text=True, timeout=30)
        return result.returncode == 0
    except Exception as e:
        logger.error("ray_stop exception: %s", e)
        return False


def _ray_is_running() -> bool:
    try:
        r = subprocess.run(
            ["ray", "status"], capture_output=True, text=True, timeout=5
        )
        return r.returncode == 0
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Heartbeat thread
# ---------------------------------------------------------------------------

class HeartbeatThread(threading.Thread):
    """Background thread: renews ownership + updates NodeRegistry."""

    MAX_RETRIES = 3

    def __init__(
        self,
        state: AgentState,
        redis_client: Any,
        node_registry: Any,
        *,
        interval: float = HEARTBEAT_INTERVAL,
    ) -> None:
        super().__init__(daemon=True, name="agent-heartbeat")
        self._state = state
        self._redis = redis_client
        self._registry = node_registry
        self._interval = interval
        self._stop_event = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        consecutive_failures = 0
        while not self._stop_event.is_set():
            try:
                ok = await renew_ownership(
                    self._redis, self._state.node_id, self._state.instance_id
                )
                if not ok:
                    logger.warning("HeartbeatThread: ownership renewal failed, retrying")
                    # Try re-acquire
                    ok = await acquire_ownership(
                        self._redis, self._state.node_id, self._state.instance_id
                    )
                    if not ok:
                        consecutive_failures += 1
                        if consecutive_failures >= self.MAX_RETRIES:
                            logger.error(
                                "HeartbeatThread: lost ownership after %d retries, stopping",
                                self.MAX_RETRIES,
                            )
                            break
                    else:
                        consecutive_failures = 0
                else:
                    consecutive_failures = 0

                # Update NodeRegistry
                if self._registry is not None:
                    node_info = self._state.to_node_info()
                    await self._registry.heartbeat(
                        self._state.node_id,
                        resources=self._state.resources.model_dump(),
                    )

            except Exception as e:
                logger.error("HeartbeatThread exception: %s", e)

            await asyncio.sleep(self._interval)


# ---------------------------------------------------------------------------
# FastAPI application factory
# ---------------------------------------------------------------------------

HEARTBEAT_INTERVAL = _HEARTBEAT_INTERVAL


def create_agent_app(
    node_id: str,
    host: str,
    agent_port: int = 9100,
    redis_client: Any = None,
    node_registry: Any = None,
) -> FastAPI:
    """Create and return the Node Agent FastAPI application."""
    state = AgentState(node_id=node_id, host=host, agent_port=agent_port)
    heartbeat: HeartbeatThread | None = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal heartbeat
        # Acquire ownership
        if redis_client:
            ok = await acquire_ownership(redis_client, node_id, state.instance_id)
            if not ok:
                logger.error("Agent: failed to acquire ownership for node %s", node_id)
            # Register node
            if node_registry:
                await node_registry.register(state.to_node_info())
            # Start heartbeat
            heartbeat = HeartbeatThread(state, redis_client, node_registry)
            heartbeat.start()
        yield
        # Cleanup
        if heartbeat:
            heartbeat.stop()
        if redis_client and state.state in (NodeState.IDLE, NodeState.RESERVED):
            await release_ownership(redis_client, node_id, state.instance_id)

    app = FastAPI(title="Ray Async Node Agent", lifespan=lifespan)

    # -----------------------------------------------------------------------
    # Health
    # -----------------------------------------------------------------------

    @app.get("/health")
    async def health():
        return {
            "node_id": state.node_id,
            "state": state.state.value,
            "cluster_id": state.cluster_id,
            "instance_id": state.instance_id,
        }

    # -----------------------------------------------------------------------
    # Invitation endpoint (Head → Agent)
    # -----------------------------------------------------------------------

    @app.post("/invite", response_model=InvitationResponse)
    async def invite(req: InvitationRequest):
        if state.state not in (NodeState.IDLE, NodeState.RESERVED):
            return InvitationResponse(
                node_id=node_id,
                accepted=False,
                reason=f"node in state {state.state.value}, cannot accept invitation",
            )
        state.transition(NodeState.JOINING, cluster_id=req.cluster_id,
                         ray_head_address=req.ray_head_address)

        success = ray_start(req.ray_head_address, custom_resources=req.custom_resources)
        if not success:
            state.transition(NodeState.IDLE, cluster_id="", ray_head_address="")
            return InvitationResponse(
                node_id=node_id, accepted=False, reason="ray start failed"
            )

        state.transition(NodeState.JOINED)
        if node_registry:
            await node_registry.mark_joined(node_id)

        return InvitationResponse(
            node_id=node_id, accepted=True, ray_node_ip=host
        )

    # -----------------------------------------------------------------------
    # Release endpoint
    # -----------------------------------------------------------------------

    @app.post("/release")
    async def release(req: ReleaseRequest):
        if req.graceful and state.state == NodeState.DRAINING:
            # Wait briefly for tasks to finish (simplified)
            await asyncio.sleep(1)
        ray_stop()
        state.transition(NodeState.IDLE, cluster_id="", ray_head_address="")
        if node_registry:
            await node_registry.mark_released(node_id)
        return {"node_id": node_id, "released": True}

    # -----------------------------------------------------------------------
    # Drain endpoint
    # -----------------------------------------------------------------------

    @app.post("/drain")
    async def drain(req: DrainRequest):
        if state.state != NodeState.JOINED:
            raise HTTPException(400, f"Cannot drain node in state {state.state.value}")
        state.transition(NodeState.DRAINING)
        if node_registry:
            await node_registry.mark_draining(node_id)
        return {"node_id": node_id, "draining": True}

    # -----------------------------------------------------------------------
    # Resources endpoint
    # -----------------------------------------------------------------------

    @app.get("/resources")
    async def get_resources():
        return state.resources.model_dump()

    # -----------------------------------------------------------------------
    # Node info endpoint
    # -----------------------------------------------------------------------

    @app.get("/node")
    async def get_node():
        return state.to_node_info().model_dump()

    # -----------------------------------------------------------------------
    # Deploy endpoint
    # -----------------------------------------------------------------------

    @app.post("/deploy", response_model=DeployResult)
    async def deploy(req: DeployRequest):
        pkg_info = DeployedPackageInfo(
            package_name=req.package_name,
            package_version=req.package_version,
            package_url=req.package_url,
            deploy_path="",
            state=DeployState.DOWNLOADING,
        )
        state.deployed_packages[req.package_name] = pkg_info

        try:
            from src.platform.remote_code_fetcher import RemoteCodeFetcher
            fetcher = RemoteCodeFetcher(allow_private_ip=True)
            local_dir = fetcher.fetch_artifact(req.package_url, req.checksum_sha256 or None)
            pkg_info.deploy_path = local_dir
            pkg_info.state = DeployState.DEPLOYED
            pkg_info.deployed_at = time.time()
            return DeployResult(
                node_id=node_id,
                package_name=req.package_name,
                package_version=req.package_version,
                state=DeployState.DEPLOYED,
                success=True,
                deploy_path=local_dir,
                deployed_at=time.time(),
            )
        except Exception as e:
            pkg_info.state = DeployState.FAILED
            return DeployResult(
                node_id=node_id,
                package_name=req.package_name,
                package_version=req.package_version,
                state=DeployState.FAILED,
                success=False,
                message=str(e),
            )

    # -----------------------------------------------------------------------
    # Undeploy endpoint
    # -----------------------------------------------------------------------

    @app.post("/undeploy")
    async def undeploy(req: UndeployRequest):
        state.deployed_packages.pop(req.package_name, None)
        return {"node_id": node_id, "package_name": req.package_name, "undeployed": True}

    # -----------------------------------------------------------------------
    # Packages list
    # -----------------------------------------------------------------------

    @app.get("/packages")
    async def list_packages():
        return {
            "packages": [p.model_dump() for p in state.deployed_packages.values()]
        }

    return app
