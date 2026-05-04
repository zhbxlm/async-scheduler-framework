"""Deploy domain models — aligned with docs/deepwiki-reference/数据模型.md"""
from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class DeployState(str, Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    EXTRACTING = "extracting"
    SETTING_UP = "setting_up"
    DEPLOYED = "deployed"
    FAILED = "failed"


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
