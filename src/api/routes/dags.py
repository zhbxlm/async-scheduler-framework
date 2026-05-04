"""dags API routes."""
from fastapi import APIRouter

router = APIRouter(prefix="/dags", tags=["dags"])


@router.get("/")
async def list_dags():
    return {"message": "dags API"}
