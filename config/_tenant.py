"""Multi-tenant config."""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class ArtifactStoreConfig:
    type: str = "local"
    base_dir: str = "/shared/artifacts"

    def __post_init__(self):
        self.type = os.getenv("ARTIFACT_STORE_TYPE", self.type)
        self.base_dir = os.getenv("ARTIFACT_STORE_BASE_DIR", self.base_dir)


@dataclass
class DefaultQuotaConfig:
    max_tasks: int = 10000
    max_gpus: int = 1000
    max_actors: int = 5000

    def __post_init__(self):
        self.max_tasks = int(os.getenv("DEFAULT_QUOTA_MAX_TASKS", self.max_tasks))
        self.max_gpus = int(os.getenv("DEFAULT_QUOTA_MAX_GPUS", self.max_gpus))
        self.max_actors = int(os.getenv("DEFAULT_QUOTA_MAX_ACTORS", self.max_actors))


@dataclass
class RateLimitConfig:
    enabled: bool = True
    per_second: int = 100
    burst: int = 200

    def __post_init__(self):
        self.enabled = os.getenv("RATE_LIMIT_ENABLED", "true").lower() == "true"
        self.per_second = int(os.getenv("RATE_LIMIT_PER_SECOND", self.per_second))
        self.burst = int(os.getenv("RATE_LIMIT_BURST", self.burst))


@dataclass
class WorkerArtifactConfig:
    max_size_mb: int = 1024
    cache_max_entries: int = 50

    def __post_init__(self):
        self.max_size_mb = int(os.getenv("WORKER_ARTIFACT_MAX_SIZE_MB", self.max_size_mb))
        self.cache_max_entries = int(os.getenv("WORKER_ARTIFACT_CACHE_MAX_ENTRIES", self.cache_max_entries))


@dataclass
class TenantConfig:
    multi_tenant_enabled: bool = False
    super_admin_api_key: str = ""
    artifact_store: ArtifactStoreConfig = field(default_factory=ArtifactStoreConfig)
    default_quota: DefaultQuotaConfig = field(default_factory=DefaultQuotaConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    worker_artifact: WorkerArtifactConfig = field(default_factory=WorkerArtifactConfig)

    def __post_init__(self):
        self.multi_tenant_enabled = os.getenv("MULTI_TENANT_ENABLED", "false").lower() == "true"
        self.super_admin_api_key = os.getenv("SUPER_ADMIN_API_KEY", self.super_admin_api_key)
