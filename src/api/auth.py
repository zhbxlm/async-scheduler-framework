"""API authentication middleware — multi-tenant API key validation."""
from __future__ import annotations
from fastapi import HTTPException, Request, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional

security = HTTPBearer(auto_error=False)


async def authenticate(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security),
) -> dict:
    """Authenticate via API key, optionally with tenant context."""
    from config.settings import settings
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing API key")
    
    api_key = credentials.credentials
    tenant_id = request.headers.get("X-Tenant-Id")
    
    # Single-tenant mode
    if not settings.tenant.multi_tenant_enabled:
        if api_key == settings.tenant.super_admin_api_key:
            return {"tenant_id": "super_admin", "api_key": api_key, "is_super_admin": True}
        # TODO: implement tenant registry lookup
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    # Multi-tenant mode
    # TODO: query TenantRegistry
    raise HTTPException(status_code=501, detail="Multi-tenant not implemented")
