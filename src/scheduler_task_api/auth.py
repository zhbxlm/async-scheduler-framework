"""task-api package-owned authentication helpers."""
from __future__ import annotations

import hmac

from fastapi import HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer


def mask_api_key(api_key: str) -> str:
    if not api_key:
        return ""
    if len(api_key) <= 8:
        return "****"
    return api_key[:4] + "***" + api_key[-4:]


security = HTTPBearer(auto_error=False)


async def authenticate(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Security(security),
) -> dict:
    app_state = getattr(request.app.state, "auth_settings", None)
    if app_state:
        super_admin_key = app_state.get("super_admin_key")
        multi_tenant_enabled = app_state.get("multi_tenant_enabled", False)
        tenant_id_header = app_state.get("tenant_id_header", "X-Tenant-ID")
    else:
        from config.settings_pydantic import settings
        super_admin_key = settings.tenant.super_admin_api_key
        multi_tenant_enabled = settings.tenant.multi_tenant_enabled
        tenant_id_header = settings.tenant.tenant_id_header

    if not credentials:
        raise HTTPException(status_code=401, detail="Missing API key")

    api_key = credentials.credentials
    tenant_id_hint = request.headers.get(tenant_id_header) or None

    if not multi_tenant_enabled:
        if super_admin_key and hmac.compare_digest(api_key, super_admin_key):
            return {
                "tenant_id": "super_admin",
                "api_key": mask_api_key(api_key),
                "is_super_admin": True,
                "quota": None,
            }
        raise HTTPException(status_code=401, detail="Invalid API key")

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Auth service unavailable (no redis)")

    if super_admin_key and hmac.compare_digest(api_key, super_admin_key):
        return {
            "tenant_id": "super_admin",
            "api_key": mask_api_key(api_key),
            "is_super_admin": True,
            "quota": None,
        }

    registry = getattr(request.app.state, "tenant_registry", None)
    if registry is None:
        from scheduler_task_api.infra.tenant_registry import TenantRegistry
        registry = TenantRegistry(redis_client)
    tenant_data = await registry.validate_key(api_key, tenant_id_hint)

    if tenant_data is None:
        raise HTTPException(status_code=401, detail="Invalid API key")

    status = tenant_data.get("status", "active")
    if status not in ("active", "ACTIVE"):
        raise HTTPException(status_code=403, detail=f"Tenant is {status}, access denied")

    return {
        "tenant_id": tenant_data.get("tenant_id", ""),
        "tenant_name": tenant_data.get("tenant_name", ""),
        "is_super_admin": False,
        "quota": tenant_data.get("quota"),
        "status": status,
    }
