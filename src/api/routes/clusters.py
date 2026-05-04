"""clusters API routes."""
from fastapi import APIRouter

router = APIRouter(prefix="/clusters", tags=["clusters"])


@router.get("/")
async def list_clusters():
    return {"message": "clusters API"}
