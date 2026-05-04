# rebuilt from deepwiki-reference alignment
"""schedules API routes — /ops/v1/schedules"""
from __future__ import annotations
from fastapi import APIRouter, HTTPException
from typing import Any

router = APIRouter(prefix="/ops/v1/schedules", tags=["schedules"])


@router.get("/")
async def list_schedules() -> list[Any]:
    """List all schedules."""
    return []


@router.get("/{item_id}")
async def get_schedules(item_id: str) -> dict:
    """Get schedules by ID."""
    raise HTTPException(status_code=404, detail="schedules not found")


@router.post("/", status_code=201)
async def create_schedules(body: dict) -> dict:
    """Create / register a new schedules entry."""
    return {"message": "ok"}


@router.delete("/{item_id}", status_code=204)
async def delete_schedules(item_id: str) -> None:
    """Remove a schedules entry."""
    pass
