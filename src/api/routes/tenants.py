"""tenants API routes."""
from fastapi import APIRouter

router = APIRouter(prefix="/tenants", tags=["tenants"])


@router.get("/")
async def list_tenants():
    return {"message": "tenants API"}
