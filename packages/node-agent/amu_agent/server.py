"""Node Agent — amu-agent

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
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException

from amu_agent._models import (
    DeployRequest,
    DeployResult,
    DeployState,
    DeployedPackageInfo,
    DrainRequest,
    InvitationRequest,
    InvitationResponse,
    NodeInfo,
    NodeResources,
    NodeState,
    ReleaseRequest,
    UndeployRequest,
)
from amu_agent._remote_fetcher import RemoteCodeFetcher

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
    """Mutable state container for the node agent (assumed single-threaded async)."""

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

    def transition(self, new_state: NodeState, **kwargs: Any) -> None:
        """Transition to a new state and set optional attributes."""
        self.state = new_state
        for k, v in kwargs.items():
            setattr(self, k, v)

    def to_node_info(self) -> NodeInfo:
        """Convert state to NodeInfo for registration."""
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
    """Acquire ownership of a node via Redis SET with NX flag."""
    key = _OWNER_KEY.format(node_id=node_id)
    result = await redis_client.set(key, instance_id, nx=True, ex=_OWNER_TTL_SECONDS)
    return bool(result)


async def renew_ownership(redis_client: Any, node_id: str, instance_id: str) -> bool:
    """Renew ownership of a node via Lua script (check-and-set TTL)."""
    key = _OWNER_KEY.format(node_id=node_id)
    result = await redis_client.eval(_LUA_RENEW, 1, key, instance_id, str(_OWNER_TTL_SECONDS))
    return result in (b"ok", "ok")


async def release_ownership(redis_client: Any, node_id: str, instance_id: str) -> bool:
    """Release ownership of a node via Lua script (check-and-delete)."""
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
    """Check if Ray is running."""
    try:
        r = subprocess.run(
            ["ray", "status"], capture_output=True, text=True, timeout=5
        )
        return r.returncode == 0
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Heartbeat background task
# ---------------------------------------------------------------------------

async def heartbeat_task(
    state: AgentState,
    redis_client: Any,
    node_registry: Any | None,
    stop_event: asyncio.Event,
    interval: float = _HEARTBEAT_INTERVAL,
) -> None:
    """Background coroutine: renews ownership + updates NodeRegistry."""
    MAX_RETRIES = 3
    consecutive_failures = 0
    while not stop_event.is_set():
        try:
            ok = await renew_ownership(
                redis_client, state.node_id, state.instance_id
            )
            if not ok:
                logger.warning("heartbeat_task: ownership renewal failed, retrying")
                # Try re-acquire
                ok = await acquire_ownership(
                    redis_client, state.node_id, state.instance_id
                )
                if not ok:
                    consecutive_failures += 1
                    if consecutive_failures >= MAX_RETRIES:
                        logger.error(
                            "heartbeat_task: lost ownership after %d retries, stopping",
                            MAX_RETRIES,
                        )
                        break
                else:
                    consecutive_failures = 0
            else:
                consecutive_failures = 0

            # Update NodeRegistry
            if node_registry is not None:
                node_info = state.to_node_info()
                await node_registry.heartbeat(
                    state.node_id,
                    resources=state.resources.model_dump(),
                )

        except Exception as e:
            logger.error("heartbeat_task exception: %s", e)

        await asyncio.sleep(interval)


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
    """Create and return the Node Agent FastAPI application.

    Parameters
    ----------
    node_id : str
        Unique identifier for this node.
    host : str
        The hostname or IP address of this node.
    agent_port : int
        Port on which the agent listens.
    redis_client : Any
        Async Redis client for ownership protocol.
    node_registry : Any | None
        Optional NodeRegistry client for registration.

    Returns
    -------
    FastAPI
        The FastAPI application.
    """
    state = AgentState(node_id=node_id, host=host, agent_port=agent_port)
    stop_event = asyncio.Event()
    heartbeat_task_handle: asyncio.Task | None = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal heartbeat_task_handle
        # Acquire ownership
        if redis_client:
            ok = await acquire_ownership(redis_client, node_id, state.instance_id)
            if not ok:
                logger.error("Agent: failed to acquire ownership for node %s", node_id)
            # Register node
            if node_registry:
                await node_registry.register(state.to_node_info())
            # Start heartbeat background task
            heartbeat_task_handle = asyncio.create_task(
                heartbeat_task(state, redis_client, node_registry, stop_event)
            )
        yield
        # Cleanup
        stop_event.set()
        if heartbeat_task_handle:
            heartbeat_task_handle.cancel()
            try:
                await heartbeat_task_handle
            except asyncio.CancelledError:
                pass
        if redis_client and state.state in (NodeState.IDLE, NodeState.RESERVED):
            await release_ownership(redis_client, node_id, state.instance_id)

    app = FastAPI(title="Ray AMU Node Agent", lifespan=lifespan)

    # -----------------------------------------------------------------------
    # Health
    # -----------------------------------------------------------------------

    @app.get("/health")
    async def health():
        """Health check endpoint."""
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
        """Handle invitation from Head to join a cluster."""
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
        """Handle release request from Head."""
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
        """Handle drain request from Head."""
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
        """Get current node resources."""
        return state.resources.model_dump()

    # -----------------------------------------------------------------------
    # Node info endpoint
    # -----------------------------------------------------------------------

    @app.get("/node")
    async def get_node():
        """Get full node info."""
        return state.to_node_info().model_dump()

    # -----------------------------------------------------------------------
    # Deploy endpoint
    # -----------------------------------------------------------------------

    @app.post("/deploy", response_model=DeployResult)
    async def deploy(req: DeployRequest):
        """Handle deployment request."""
        pkg_info = DeployedPackageInfo(
            package_name=req.package_name,
            package_version=req.package_version,
            package_url=req.package_url,
            deploy_path="",
            state=DeployState.DOWNLOADING,
        )
        state.deployed_packages[req.package_name] = pkg_info

        try:
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
        """Handle undeployment request."""
        state.deployed_packages.pop(req.package_name, None)
        return {"node_id": node_id, "package_name": req.package_name, "undeployed": True}

    # -----------------------------------------------------------------------
    # Packages list
    # -----------------------------------------------------------------------

    @app.get("/packages")
    async def list_packages():
        """List deployed packages."""
        return {
            "packages": [p.model_dump() for p in state.deployed_packages.values()]
        }

    return app


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    """Main entry point for running the agent from command line."""
    import uvicorn
    from amu_agent.config import AgentConfig

    logging.basicConfig(
        level=getattr(logging, AgentConfig.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    redis_client = None
    try:
        import redis.asyncio as redis_aioredis
        redis_client = redis_aioredis.from_url(AgentConfig.redis_url)
    except ImportError:
        logger.warning("redis-async not installed, ownership protocol disabled")

    app = create_agent_app(
        node_id=AgentConfig.node_id,
        host=AgentConfig.host,
        agent_port=AgentConfig.port,
        redis_client=redis_client,
    )

    uvicorn.run(
        app,
        host=AgentConfig.host,
        port=AgentConfig.port,
        log_level=AgentConfig.log_level.lower(),
    )


if __name__ == "__main__":
    main()
