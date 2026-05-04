"""API authentication middleware — aligned with docs/deepwiki-reference/配额与多租户.md

Single-tenant mode:  validates against super_admin_api_key env var
Multi-tenant mode:   SHA-256 API key hash lookup via TenantRegistry
                     Checks tenant status == ACTIVE before allowing
"""
from __future__ import annotations

from typing import Optional

from fastapi import HTTPException, Request, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer(auto_error=False)


async def authenticate(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security),
) -> dict:
    """Authenticate via API key; return tenant context dict."""
    from config.settings_compat import settings

    if not credentials:
        raise HTTPException(status_code=401, detail="Missing API key")

    api_key = credentials.credentials
    tenant_id_header = request.headers.get(settings.tenant.tenant_id_header)

    # ----------------------------------------------------------------
    # Single-tenant mode
    # ----------------------------------------------------------------
    if not settings.tenant.multi_tenant_enabled:
        expected = settings.tenant.super_admin_api_key
        if expected and api_key == expected:
            return {
                "tenant_id": "super_admin",
                "api_key": api_key,
                "is_super_admin": True,
                "quota": None,
            }
        raise HTTPException(status_code=401, detail="Invalid API key")

    # ----------------------------------------------------------------
    # Multi-tenant mode — query TenantRegistry
    # ----------------------------------------------------------------
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Auth service unavailable (no redis)")

    # Super-admin bypass
    super_key = settings.tenant.super_admin_api_key
    if super_key and api_key == super_key:
        return {
            "tenant_id": "super_admin",
            "api_key": api_key,
            "is_super_admin": True,
            "quota": None,
        }

    # Reuse registry from app.state (initialised at startup) to avoid
    # re-instantiating TenantRegistry on every request.
    registry = getattr(request.app.state, "tenant_registry", None)
    if registry is None:
        # Fallback for tests or when app.state is not populated
        from src.platform.tenant_registry import TenantRegistry
        registry = TenantRegistry(redis_client)
    tenant_data = await registry.validate_key(api_key, tenant_id_header)

    if tenant_data is None:
        raise HTTPException(status_code=401, detail="Invalid API key")

    # Check tenant status
    status = tenant_data.get("status", "active")
    if status not in ("active", "ACTIVE"):
        raise HTTPException(
            status_code=403,
            detail=f"Tenant is {status}, access denied",
        )

    return {
        "tenant_id": tenant_data.get("tenant_id", ""),
        "tenant_name": tenant_data.get("tenant_name", ""),
        "is_super_admin": False,
        "quota": tenant_data.get("quota"),
        "status": status,
    }
