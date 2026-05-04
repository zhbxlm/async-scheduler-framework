"""Capability domain models — aligned with docs/deepwiki-reference/数据模型.md"""
from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class CapabilityType(str, Enum):
    RAY_ACTOR = "ray_actor"
    RAY_SERVE = "ray_serve"
    FLASK_HTTP = "flask_http"
    RAY_TASK = "ray_task"


class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


# ---------------------------------------------------------------------------
# Config sub-models
# ---------------------------------------------------------------------------

class ActorConfig(BaseModel):
    """Ray Actor type capability configuration."""

    # Entry & artifact
    module_path: str = ""
    artifact_url: str = ""
    artifact_sha256: str = ""
    entrypoint: str = ""

    # Pool management
    num_actors: int = Field(default=1, ge=1)
    auto_create_pool: bool = True
    elastic_enabled: bool = True
    warmup_enabled: bool = False
    warmup_payload: dict[str, Any] = Field(default_factory=lambda: {"_warmup": True})
    warmup_timeout_seconds: int = 120

    # Resource config
    resources_per_actor: dict[str, Any] = Field(default_factory=dict)
    actor_options: dict[str, Any] = Field(default_factory=dict)
    worker_init_config: dict[str, Any] = Field(default_factory=dict)
    runtime_env: dict[str, Any] = Field(default_factory=dict)
    scheduling_resources: dict[str, float] = Field(default_factory=dict)

    # Rate limiting & tidal
    qps_limit: float | None = None
    max_concurrent: int | None = None
    preemptible: bool = False
    preemptible_min_size: int = 0

    @model_validator(mode="after")
    def validate_artifact_and_limits(self) -> "ActorConfig":
        if self.artifact_url:
            if not self.entrypoint:
                raise ValueError("entrypoint is required when artifact_url is set")
            if self.artifact_sha256 and len(self.artifact_sha256) != 64:
                raise ValueError("artifact_sha256 must be a 64-char hex string")
        if self.qps_limit is not None and self.qps_limit <= 0:
            raise ValueError("qps_limit must be positive")
        if self.max_concurrent is not None and self.max_concurrent <= 0:
            raise ValueError("max_concurrent must be positive")
        return self


class FlaskConfig(BaseModel):
    """Flask HTTP type capability configuration."""
    base_url: str = ""
    generate_endpoint: str = ""
    health_endpoint: str = "/health"


class HealthCheckConfig(BaseModel):
    """Capability health check configuration."""
    interval_seconds: int = 30
    timeout_seconds: int = 10
    unhealthy_threshold: int = 3
    healthy_threshold: int = 2


# ---------------------------------------------------------------------------
# Core model
# ---------------------------------------------------------------------------

class CapabilityInfo(BaseModel):
    """Full capability registration info."""
    capability_name: str
    capability_type: CapabilityType = CapabilityType.RAY_ACTOR
    cluster_id: str = ""
    version: str = "1.0.0"
    actor_config: ActorConfig | None = None
    flask_config: FlaskConfig | None = None
    health_check: HealthCheckConfig = Field(default_factory=HealthCheckConfig)
    health_status: HealthStatus = HealthStatus.HEALTHY
    tags: list[str] = Field(default_factory=list)
    tenant_id: str = ""
    description: str = ""
