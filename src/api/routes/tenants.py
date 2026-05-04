# rebuilt from deepwiki-reference alignment
"""tenants API routes — /ops/v1/tenants"""
from __future__ import annotations
from fastapi import APIRouter, HTTPException
from typing import Any

router = APIRouter(prefix="/ops/v1/tenants", tags=["tenants"])


@router.get("/")
async def list_tenants() -> list[Any]:
    """List all tenants."""
    return []


@router.get("/{item_id}")
async def get_tenants(item_id: str) -> dict:
    """Get tenants by ID."""
    raise HTTPException(status_code=404, detail="tenants not found")


@router.post("/", status_code=201)
async def create_tenants(body: dict) -> dict:
    """Create / register a new tenants entry."""
    return {"message": "ok"}


@router.delete("/{item_id}", status_code=204)
async def delete_tenants(item_id: str) -> None:
    """Remove a tenants entry."""
    pass
