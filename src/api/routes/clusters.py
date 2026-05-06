"""clusters routes — /ops/v1/clusters
aligned with docs/deepwiki-reference/API 参考.md + 调度与资源管理.md
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from src.api.auth import authenticate

router = APIRouter(prefix="/ops/v1/clusters", tags=["clusters"])


def _registry(request: Request):
    reg = getattr(request.app.state, "cluster_registry", None)
    if reg is None:
        raise HTTPException(status_code=503, detail="ClusterRegistry not initialised")
    return reg


@router.get("/", summary="List all clusters")
async def list_clusters(
    request: Request,
    tenant_id: str = "",
    _auth: dict = Depends(authenticate),
) -> list[dict]:
    reg = _registry(request)
    tenant = _auth.get("tenant_id", tenant_id or "default")
    ids = await reg.list(tenant)
    clusters = []
    for cid in ids:
        cl = await reg.get(tenant, cid)
        if cl:
            clusters.append(cl)
    return clusters


@router.get("/{cluster_id}", summary="Get cluster detail")
async def get_cluster(
    cluster_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = _auth.get("tenant_id", "default")
    cl = await reg.get(tenant, cluster_id)
    if cl is None:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_id!r} not found")
    return cl


@router.post("/", status_code=201, summary="Register cluster")
async def register_cluster(
    request: Request,
    body: dict,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = _auth.get("tenant_id", "default")
    cluster_id = body.get("cluster_id")
    if not cluster_id:
        raise HTTPException(status_code=422, detail="cluster_id required")
    await reg.set(tenant, cluster_id, body)
    return {"cluster_id": cluster_id, "registered": True}


@router.put("/{cluster_id}", summary="Update cluster")
async def update_cluster(
    cluster_id: str,
    request: Request,
    body: dict,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = _auth.get("tenant_id", "default")
    existing = await reg.get(tenant, cluster_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_id!r} not found")
    merged = {**existing, **body, "cluster_id": cluster_id}
    await reg.set(tenant, cluster_id, merged)
    return merged


@router.delete("/{cluster_id}", status_code=204, summary="Unregister cluster")
async def delete_cluster(
    cluster_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> None:
    reg = _registry(request)
    tenant = _auth.get("tenant_id", "default")
    await reg.delete(tenant, cluster_id)


@router.get("/{cluster_id}/resources", summary="Get cluster observed resources")
async def get_cluster_resources(
    cluster_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = _auth.get("tenant_id", "default")
    cl = await reg.get(tenant, cluster_id)
    if cl is None:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_id!r} not found")
    return {
        "cluster_id": cluster_id,
        "observed": cl.get("observed_resources", {}),
        "planned": cl.get("planned_resources", {}),
    }
