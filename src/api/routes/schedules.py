"""schedules API routes."""
from fastapi import APIRouter

router = APIRouter(prefix="/schedules", tags=["schedules"])


@router.get("/")
async def list_schedules():
    return {"message": "schedules API"}
