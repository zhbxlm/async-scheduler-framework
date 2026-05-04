"""nodes API routes."""
from fastapi import APIRouter

router = APIRouter(prefix="/nodes", tags=["nodes"])


@router.get("/")
async def list_nodes():
    return {"message": "nodes API"}
