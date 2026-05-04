"""ops API routes."""
from fastapi import APIRouter

router = APIRouter(prefix="/ops", tags=["ops"])


@router.get("/")
async def list_ops():
    return {"message": "ops API"}
