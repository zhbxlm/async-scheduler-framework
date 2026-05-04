"""TenantContext — resolved per-request tenant identity."""
from __future__ import annotations
from dataclasses import dataclass


@dataclass
class TenantContext:
    tenant_id: str
    api_key: str = ""
    is_super_admin: bool = False
    quota_max_tasks: int = 0
    quota_max_gpus: int = 0
    quota_max_actors: int = 0
