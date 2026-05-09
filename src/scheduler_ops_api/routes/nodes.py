"""nodes routes — /ops/v1/nodes
aligned with docs/deepwiki-reference/API 参考.md + 节点代理.md
"""
from __future__ import annotations

from fastapi import APIRouter, Depends

from scheduler_ops_api.auth import authenticate
from scheduler_ops_api.dependencies import NodeRegistryDep
from src.services.access_policy import resolve_tenant_id
from src.services.resource_application import NodeService

router = APIRouter(prefix="/ops/v1/nodes", tags=["nodes"])


@router.get("/", summary="List all nodes")
async def list_nodes(
    registry: NodeRegistryDep,
    cluster_id: str = "",
    _auth: dict = Depends(authenticate),
) -> list[dict]:
    tenant = resolve_tenant_id(_auth)
    return await NodeService(registry).list_nodes(tenant, cluster_id)


@router.get("/{node_id}", summary="Get node detail")
async def get_node(
    node_id: str,
    registry: NodeRegistryDep,
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth)
    return await NodeService(registry).get_node(tenant, node_id)


@router.post("/{node_id}/invite", summary="Invite node to join cluster")
async def invite_node(
    node_id: str,
    body: dict,
    registry: NodeRegistryDep,
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth)
    return await NodeService(registry).invite_node(tenant, node_id, body)


@router.post("/{node_id}/drain", summary="Start draining node")
async def drain_node(
    node_id: str,
    body: dict,
    registry: NodeRegistryDep,
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth)
    return await NodeService(registry).drain_node(tenant, node_id, body)


@router.post("/{node_id}/release", summary="Release node from cluster")
async def release_node(
    node_id: str,
    registry: NodeRegistryDep,
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth)
    return await NodeService(registry).release_node(tenant, node_id)


@router.delete("/{node_id}", status_code=204, summary="Unregister node")
async def delete_node(
    node_id: str,
    registry: NodeRegistryDep,
    _auth: dict = Depends(authenticate),
) -> None:
    tenant = resolve_tenant_id(_auth)
    await NodeService(registry).delete_node(tenant, node_id)
