from __future__ import annotations

from fastapi import HTTPException


def resolve_tenant_id(auth: dict, fallback: str = "default") -> str:
    """Extract caller tenant_id from auth context, with an optional fallback."""
    return auth.get("tenant_id", fallback) or fallback


class TenantAccessPolicy:
    @staticmethod
    def require_super_admin(auth: dict) -> None:
        if not auth.get("is_super_admin"):
            raise HTTPException(status_code=403, detail="Super-admin only")

    @staticmethod
    def require_same_tenant_or_super_admin(auth: dict, tenant_id: str) -> None:
        caller_tid = auth.get("tenant_id", "")
        if not auth.get("is_super_admin") and caller_tid != tenant_id:
            raise HTTPException(status_code=403, detail="Access denied")
