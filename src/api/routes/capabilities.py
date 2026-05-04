"""capabilities API routes."""
from fastapi import APIRouter

router = APIRouter(prefix="/capabilities", tags=["capabilities"])


@router.get("/")
async def list_capabilities():
    return {"message": "capabilities API"}
