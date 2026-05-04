# rebuilt from deepwiki-reference alignment
"""capabilities API routes — /ops/v1/capabilities"""
from __future__ import annotations
from fastapi import APIRouter, HTTPException
from typing import Any

router = APIRouter(prefix="/ops/v1/capabilities", tags=["capabilities"])


@router.get("/")
async def list_capabilities() -> list[Any]:
    """List all capabilities."""
    return []


@router.get("/{item_id}")
async def get_capabilities(item_id: str) -> dict:
    """Get capabilities by ID."""
    raise HTTPException(status_code=404, detail="capabilities not found")


@router.post("/", status_code=201)
async def create_capabilities(body: dict) -> dict:
    """Create / register a new capabilities entry."""
    return {"message": "ok"}


@router.delete("/{item_id}", status_code=204)
async def delete_capabilities(item_id: str) -> None:
    """Remove a capabilities entry."""
    pass
