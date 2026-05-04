# rebuilt from deepwiki-reference alignment
"""Multi-tenancy config."""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class TenantConfig:
    multi_tenant_enabled: bool = False
    super_admin_api_key: str = ""
    default_max_tasks: int = 100
    default_max_gpus: int = 8
    default_max_actors: int = 4
    api_key_header: str = "X-API-Key"
    tenant_id_header: str = "X-Tenant-Id"

    def __post_init__(self):
        self.multi_tenant_enabled = os.getenv("MULTI_TENANT_ENABLED", str(self.multi_tenant_enabled)).lower() == "true"
        self.super_admin_api_key  = os.getenv("SUPER_ADMIN_API_KEY", os.getenv("RAY_ASYNC_API_KEY", self.super_admin_api_key))
        self.default_max_tasks    = int(os.getenv("DEFAULT_MAX_TASKS", self.default_max_tasks))
        self.default_max_gpus     = int(os.getenv("DEFAULT_MAX_GPUS", self.default_max_gpus))
        self.default_max_actors   = int(os.getenv("DEFAULT_MAX_ACTORS", self.default_max_actors))
        self.api_key_header       = os.getenv("API_KEY_HEADER", self.api_key_header)
        self.tenant_id_header     = os.getenv("TENANT_ID_HEADER", self.tenant_id_header)
