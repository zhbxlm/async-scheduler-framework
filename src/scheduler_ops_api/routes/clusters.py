"""clusters routes — /ops/v1/clusters
aligned with docs/deepwiki-reference/API 参考.md + 调度与资源管理.md
"""
from __future__ import annotations

from fastapi import APIRouter, Depends

from scheduler_ops_api.auth import authenticate
from scheduler_ops_api.dependencies import ClusterRegistryDep
from src.services.access_policy import resolve_tenant_id
from src.services.resource_application import ClusterService

router = APIRouter(prefix="/ops/v1/clusters", tags=["clusters"])


@router.get("/", summary="List all clusters")
async def list_clusters(
    registry: ClusterRegistryDep,
    tenant_id: str = "",
    _auth: dict = Depends(authenticate),
) -> list[dict]:
    tenant = resolve_tenant_id(_auth, tenant_id or "default")
    return await ClusterService(registry).list_clusters(tenant)


@router.get("/{cluster_id}", summary="Get cluster detail")
async def get_cluster(
    cluster_id: str,
    registry: ClusterRegistryDep,
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth)
    return await ClusterService(registry).get_cluster(tenant, cluster_id)


@router.post("/", status_code=201, summary="Register cluster")
async def register_cluster(
    body: dict,
    registry: ClusterRegistryDep,
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth)
    return await ClusterService(registry).register_cluster(tenant, body)


@router.put("/{cluster_id}", summary="Update cluster")
async def update_cluster(
    cluster_id: str,
    body: dict,
    registry: ClusterRegistryDep,
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth)
    return await ClusterService(registry).update_cluster(tenant, cluster_id, body)


@router.delete("/{cluster_id}", status_code=204, summary="Unregister cluster")
async def delete_cluster(
    cluster_id: str,
    registry: ClusterRegistryDep,
    _auth: dict = Depends(authenticate),
) -> None:
    tenant = resolve_tenant_id(_auth)
    await ClusterService(registry).delete_cluster(tenant, cluster_id)


@router.get("/{cluster_id}/resources", summary="Get cluster observed resources")
async def get_cluster_resources(
    cluster_id: str,
    registry: ClusterRegistryDep,
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth)
    return await ClusterService(registry).get_cluster_resources(tenant, cluster_id)
