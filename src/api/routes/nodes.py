# rebuilt from deepwiki-reference alignment
"""nodes API routes — /ops/v1/nodes"""
from __future__ import annotations
from fastapi import APIRouter, HTTPException
from typing import Any

router = APIRouter(prefix="/ops/v1/nodes", tags=["nodes"])


@router.get("/")
async def list_nodes() -> list[Any]:
    """List all nodes."""
    return []


@router.get("/{item_id}")
async def get_nodes(item_id: str) -> dict:
    """Get nodes by ID."""
    raise HTTPException(status_code=404, detail="nodes not found")


@router.post("/", status_code=201)
async def create_nodes(body: dict) -> dict:
    """Create / register a new nodes entry."""
    return {"message": "ok"}


@router.delete("/{item_id}", status_code=204)
async def delete_nodes(item_id: str) -> None:
    """Remove a nodes entry."""
    pass
