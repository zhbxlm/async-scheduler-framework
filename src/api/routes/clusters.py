# rebuilt from deepwiki-reference alignment
"""clusters API routes — /ops/v1/clusters"""
from __future__ import annotations
from fastapi import APIRouter, HTTPException
from typing import Any

router = APIRouter(prefix="/ops/v1/clusters", tags=["clusters"])


@router.get("/")
async def list_clusters() -> list[Any]:
    """List all clusters."""
    return []


@router.get("/{item_id}")
async def get_clusters(item_id: str) -> dict:
    """Get clusters by ID."""
    raise HTTPException(status_code=404, detail="clusters not found")


@router.post("/", status_code=201)
async def create_clusters(body: dict) -> dict:
    """Create / register a new clusters entry."""
    return {"message": "ok"}


@router.delete("/{item_id}", status_code=204)
async def delete_clusters(item_id: str) -> None:
    """Remove a clusters entry."""
    pass
