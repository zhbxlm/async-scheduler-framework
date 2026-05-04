# rebuilt from deepwiki-reference alignment
"""dags API routes — /ops/v1/dags"""
from __future__ import annotations
from fastapi import APIRouter, HTTPException
from typing import Any

router = APIRouter(prefix="/ops/v1/dags", tags=["dags"])


@router.get("/")
async def list_dags() -> list[Any]:
    """List all dags."""
    return []


@router.get("/{item_id}")
async def get_dags(item_id: str) -> dict:
    """Get dags by ID."""
    raise HTTPException(status_code=404, detail="dags not found")


@router.post("/", status_code=201)
async def create_dags(body: dict) -> dict:
    """Create / register a new dags entry."""
    return {"message": "ok"}


@router.delete("/{item_id}", status_code=204)
async def delete_dags(item_id: str) -> None:
    """Remove a dags entry."""
    pass
