"""dags routes — /ops/v1/dags
aligned with docs/deepwiki-reference/API 参考.md + DAG 编排.md
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from src.api.auth import authenticate

router = APIRouter(prefix="/ops/v1/dags", tags=["dags"])


def _loader(request: Request):
    loader = getattr(request.app.state, "dag_loader", None)
    if loader is None:
        raise HTTPException(status_code=503, detail="DagLoader not initialised")
    return loader


@router.get("/", summary="List registered DAGs")
async def list_dags(
    request: Request,
    tenant_id: str = "default",
    _auth: dict = Depends(authenticate),
) -> list[dict]:
    """List DAG IDs via Redis scan (dag_def:{tenant_id}:* keys)."""
    redis = getattr(request.app.state, "redis", None)
    if redis is None:
        return []
    tenant = _auth.get("tenant_id", tenant_id)
    pattern = f"dag_def:{tenant}:*"
    dags = []
    async for key in redis.scan_iter(pattern):
        key_str = key.decode() if isinstance(key, bytes) else key
        dag_id = key_str.split(":")[-1]
        dags.append({"dag_id": dag_id, "tenant_id": tenant})
    return dags


@router.get("/{dag_id}", summary="Get DAG definition")
async def get_dag(
    dag_id: str,
    request: Request,
    tenant_id: str = "default",
    _auth: dict = Depends(authenticate),
) -> dict:
    loader = _loader(request)
    tenant = _auth.get("tenant_id", tenant_id)
    dag = await loader.load(dag_id, tenant)
    if dag is None:
        raise HTTPException(status_code=404, detail=f"DAG {dag_id!r} not found")
    return dag


@router.post("/", status_code=201, summary="Register DAG definition")
async def register_dag(
    request: Request,
    body: dict,
    _auth: dict = Depends(authenticate),
) -> dict:
    loader = _loader(request)
    tenant = _auth.get("tenant_id", "default")
    dag_id = body.get("dag_id")
    if not dag_id:
        raise HTTPException(status_code=422, detail="dag_id required")
    await loader.register(dag_id, tenant, body)
    return {"dag_id": dag_id, "tenant_id": tenant, "registered": True}


@router.put("/{dag_id}", summary="Update DAG definition")
async def update_dag(
    dag_id: str,
    request: Request,
    body: dict,
    _auth: dict = Depends(authenticate),
) -> dict:
    loader = _loader(request)
    tenant = _auth.get("tenant_id", "default")
    existing = await loader.load(dag_id, tenant)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"DAG {dag_id!r} not found")
    merged = {**existing, **body, "dag_id": dag_id}
    await loader.register(dag_id, tenant, merged)
    return merged


@router.delete("/{dag_id}", status_code=204, summary="Delete DAG definition")
async def delete_dag(
    dag_id: str,
    request: Request,
    tenant_id: str = "default",
    _auth: dict = Depends(authenticate),
) -> None:
    loader = _loader(request)
    tenant = _auth.get("tenant_id", tenant_id)
    await loader.delete(dag_id, tenant)
