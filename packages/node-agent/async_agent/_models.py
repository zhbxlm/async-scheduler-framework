"""Pydantic models for async-agent — extracted from src/models/node.py and src/models/deploy.py.

This module contains only the data models needed by the Node Agent,
with no SQLAlchemy or other framework dependencies.
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class NodeState(str, Enum):
    IDLE = "idle"
    RESERVED = "reserved"
    JOINING = "joining"
    JOINED = "joined"
    DRAINING = "draining"
    OFFLINE = "offline"


class DeployState(str, Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    EXTRACTING = "extracting"
    SETTING_UP = "setting_up"
    DEPLOYED = "deployed"
    FAILED = "failed"


# ---------------------------------------------------------------------------
# Resource models
# ---------------------------------------------------------------------------

class GPUInfo(BaseModel):
    """Single GPU information."""
    gpu_index: int = 0
    gpu_type: str = ""
    memory_total_mb: int = 0
    memory_used_mb: int = 0


class NodeResources(BaseModel):
    """Node resource info reported by Agent."""

    # Physical resources (locally probed)
    total_cpus: int = 0
    available_cpus: int = 0
    total_memory_mb: int = 0
    available_memory_mb: int = 0
    total_gpus: int = 0
    available_gpus: int = 0
    gpu_list: list[GPUInfo] = Field(default_factory=list)

    # Ray resource sync (filled when node has joined cluster)
    ray_node_id: str = ""
    ray_resources_total: dict[str, float] = Field(default_factory=dict)
    ray_resources_available: dict[str, float] = Field(default_factory=dict)
    ray_sync_time: float = 0.0

    @property
    def ray_sync_time_iso(self) -> str:
        if self.ray_sync_time == 0.0:
            return ""
        return datetime.fromtimestamp(self.ray_sync_time, tz=timezone.utc).isoformat()

    @property
    def is_ray_synced(self) -> bool:
        return self.ray_sync_time > 0.0 and bool(self.ray_resources_total)

    def get_effective_cpus(self) -> int:
        """Return effective CPUs — prefer Ray data when synced."""
        if self.is_ray_synced:
            return int(self.ray_resources_total.get("CPU", self.total_cpus))
        return self.total_cpus

    def get_effective_memory_mb(self) -> int:
        """Return effective memory MB — prefer Ray data when synced."""
        if self.is_ray_synced:
            mem_bytes = self.ray_resources_total.get("memory", 0)
            return int(mem_bytes / (1024 * 1024)) if mem_bytes else self.total_memory_mb
        return self.total_memory_mb

    def get_effective_gpus(self) -> int:
        """Return effective GPU count — prefer Ray data when synced."""
        if self.is_ray_synced:
            return int(self.ray_resources_total.get("GPU", self.total_gpus))
        return self.total_gpus


# ---------------------------------------------------------------------------
# Core node model
# ---------------------------------------------------------------------------

class NodeInfo(BaseModel):
    """Full node registration info."""

    # Identity & network
    node_id: str
    host: str = ""
    agent_port: int = 9100

    # State & resources
    state: NodeState = NodeState.IDLE
    resources: NodeResources = Field(default_factory=NodeResources)

    # Labels & scheduling
    labels: dict[str, str] = Field(default_factory=dict)
    custom_resources: dict[str, float] = Field(default_factory=dict)

    # Cluster association
    cluster_id: str = ""
    ray_head_address: str = ""

    # Timestamps
    last_heartbeat: float = 0.0
    registered_at: float = 0.0
    tenant_id: str = ""

    @property
    def last_heartbeat_iso(self) -> str:
        if self.last_heartbeat == 0.0:
            return ""
        return datetime.fromtimestamp(self.last_heartbeat, tz=timezone.utc).isoformat()

    @property
    def registered_at_iso(self) -> str:
        if self.registered_at == 0.0:
            return ""
        return datetime.fromtimestamp(self.registered_at, tz=timezone.utc).isoformat()

    @property
    def agent_url(self) -> str:
        return f"http://{self.host}:{self.agent_port}"


# ---------------------------------------------------------------------------
# Node protocol request/response models
# ---------------------------------------------------------------------------

class InvitationRequest(BaseModel):
    cluster_id: str
    ray_head_address: str
    labels: dict[str, str] = Field(default_factory=dict)
    custom_resources: dict[str, float] = Field(default_factory=dict)


class InvitationResponse(BaseModel):
    node_id: str
    accepted: bool = False
    reason: str = ""
    ray_node_ip: str = ""


class ReleaseRequest(BaseModel):
    cluster_id: str
    reason: str = ""
    graceful: bool = True


class DrainRequest(BaseModel):
    cluster_id: str
    deadline_seconds: int = 300


# ---------------------------------------------------------------------------
# Deploy models
# ---------------------------------------------------------------------------

class DeployRequest(BaseModel):
    """Head → Agent deploy request."""
    package_url: str
    package_name: str
    package_version: str = ""
    force: bool = False
    checksum_sha256: str = ""
    setup_timeout_seconds: int = 300


class DeployResult(BaseModel):
    """Agent → Head deploy result."""
    node_id: str
    package_name: str
    package_version: str = ""
    state: DeployState = DeployState.PENDING
    success: bool = False
    message: str = ""
    deploy_path: str = ""
    deployed_at: float = 0.0


class UndeployRequest(BaseModel):
    """Head → Agent undeploy request."""
    package_name: str


class DeployedPackageInfo(BaseModel):
    """Agent-local tracking info for a deployed package."""
    package_name: str
    package_version: str
    package_url: str
    deploy_path: str
    state: DeployState
    deployed_at: float = 0.0
    manifest: dict = Field(default_factory=dict)


class MountStatus(BaseModel):
    """AFS mount status."""
    mounted: bool = False
    mount_point: str = ""
    mount_command: str = ""
    mounted_at: float = 0.0
    error: str = ""
