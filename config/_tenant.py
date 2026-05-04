"""config/_tenant.py — Multi-tenancy config.
aligned with docs/deepwiki-reference/配置说明.md
"""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class TenantConfig:
    multi_tenant_enabled: bool = False
    super_admin_api_key: str = field(default_factory=lambda: os.getenv("SUPER_ADMIN_API_KEY", os.getenv("RAY_ASYNC_API_KEY", "")))
    default_max_gpus: int = field(default_factory=lambda: int(os.getenv("DEFAULT_MAX_GPUS", "0")))
    default_max_queue_depth: int = field(default_factory=lambda: int(os.getenv("DEFAULT_TENANT_MAX_QUEUE_DEPTH", "0")))
    default_max_concurrent_tasks: int = field(default_factory=lambda: int(os.getenv("DEFAULT_MAX_CONCURRENT_TASKS", "0")))
    worker_artifact_max_size_mb: int = field(default_factory=lambda: int(os.getenv("WORKER_ARTIFACT_MAX_SIZE_MB", "1024")))
    worker_artifact_cache_max_entries: int = field(default_factory=lambda: int(os.getenv("WORKER_ARTIFACT_CACHE_MAX_ENTRIES", "50")))
    api_key_header: str = field(default_factory=lambda: os.getenv("API_KEY_HEADER", "X-API-Key"))
    tenant_id_header: str = field(default_factory=lambda: os.getenv("TENANT_ID_HEADER", "X-Tenant-Id"))

    def __post_init__(self):
        self.multi_tenant_enabled = os.getenv("MULTI_TENANT_ENABLED", "false").lower() == "true"
