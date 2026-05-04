"""Cluster domain models — aligned with docs/deepwiki-reference/数据模型.md"""
from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_serializer, model_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class ClusterType(str, Enum):
    ONLINE_INFERENCE = "online_inference"
    OFFLINE_GENERATION = "offline_generation"
    TRAINING = "training"
    DATA_PROCESSING = "data_processing"


class ClusterStatus(str, Enum):
    ACTIVE = "active"
    DRAINING = "draining"
    OFFLINE = "offline"


# ---------------------------------------------------------------------------
# Resource models
# ---------------------------------------------------------------------------

class PlannedResources(BaseModel):
    """Cluster planned resources declared at registration time."""
    fixed_gpus: int = 0
    tidal_gpus: int = 0
    gpu_types: dict[str, int] = Field(default_factory=dict)
    total_cpus: int = 0
    total_memory_gb: float = 0.0


class ObservedResources(BaseModel):
    """Cluster runtime resources aggregated from joined nodes."""
    total_gpus: int = 0
    available_gpus: int = 0
    available_cpus: int = 0
    available_memory_gb: float = 0.0


class ClusterResources(BaseModel):
    """Cluster resource info with planned + observed layers.

    Accepts a flat dict (model_validator), exposes proxied @property access,
    and serialises back to a flat dict (model_serializer).
    """

    plan: PlannedResources = Field(default_factory=PlannedResources)
    observed: ObservedResources = Field(default_factory=ObservedResources)

    @model_validator(mode="before")
    @classmethod
    def from_flat(cls, data: Any) -> Any:
        """Accept flat dict with planned + observed fields mixed together."""
        if not isinstance(data, dict):
            return data
        if "plan" in data or "observed" in data:
            return data  # already nested
        plan_keys = {"fixed_gpus", "tidal_gpus", "gpu_types", "total_cpus", "total_memory_gb"}
        obs_keys = {"total_gpus", "available_gpus", "available_cpus", "available_memory_gb"}
        plan = {k: v for k, v in data.items() if k in plan_keys}
        obs = {k: v for k, v in data.items() if k in obs_keys}
        return {"plan": plan, "observed": obs}

    @model_serializer
    def to_flat(self) -> dict[str, Any]:
        """Serialise back to flat dict."""
        d: dict[str, Any] = {}
        d.update(self.plan.model_dump())
        d.update(self.observed.model_dump())
        return d

    # Proxy properties — planned
    @property
    def fixed_gpus(self) -> int:
        return self.plan.fixed_gpus

    @property
    def tidal_gpus(self) -> int:
        return self.plan.tidal_gpus

    @property
    def gpu_types(self) -> dict[str, int]:
        return self.plan.gpu_types

    @property
    def total_cpus(self) -> int:
        return self.plan.total_cpus

    @property
    def total_memory_gb(self) -> float:
        return self.plan.total_memory_gb

    # Proxy properties — observed
    @property
    def total_gpus(self) -> int:
        return self.observed.total_gpus

    @property
    def available_gpus(self) -> int:
        return self.observed.available_gpus

    @property
    def available_cpus(self) -> int:
        return self.observed.available_cpus

    @property
    def available_memory_gb(self) -> float:
        return self.observed.available_memory_gb


# ---------------------------------------------------------------------------
# Core model
# ---------------------------------------------------------------------------

class ClusterInfo(BaseModel):
    """Full cluster registration info."""
    cluster_id: str
    tenant_id: str = ""
    cluster_type: ClusterType = ClusterType.OFFLINE_GENERATION
    ray_head_address: str = ""
    resources: ClusterResources = Field(default_factory=ClusterResources)
    capabilities: list[str] = Field(default_factory=list)
    status: ClusterStatus = ClusterStatus.ACTIVE
    labels: dict[str, str] = Field(default_factory=dict)
