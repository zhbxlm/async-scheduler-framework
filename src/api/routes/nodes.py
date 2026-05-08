"""nodes routes — /ops/v1/nodes
aligned with docs/deepwiki-reference/API 参考.md + 节点代理.md
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from src.api.auth import authenticate
from src.services.access_policy import resolve_tenant_id

router = APIRouter(prefix="/ops/v1/nodes", tags=["nodes"])


def _registry(request: Request):
    reg = getattr(request.app.state, "node_registry", None)
    if reg is None:
        raise HTTPException(status_code=503, detail="NodeRegistry not initialised")
    return reg


@router.get("/", summary="List all nodes")
async def list_nodes(
    request: Request,
    cluster_id: str = "",
    _auth: dict = Depends(authenticate),
) -> list[dict]:
    reg = _registry(request)
    tenant = resolve_tenant_id(_auth)
    ids = await reg.list(tenant)
    nodes = []
    for nid in ids:
        node = await reg.get(tenant, nid)
        if node:
            if cluster_id and node.get("cluster_id") != cluster_id:
                continue
            nodes.append(node)
    return nodes


@router.get("/{node_id}", summary="Get node detail")
async def get_node(
    node_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = resolve_tenant_id(_auth)
    node = await reg.get(tenant, node_id)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Node {node_id!r} not found")
    return node


@router.post("/{node_id}/invite", summary="Invite node to join cluster")
async def invite_node(
    node_id: str,
    request: Request,
    body: dict,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = resolve_tenant_id(_auth)
    node = await reg.get(tenant, node_id)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Node {node_id!r} not found")
    cluster_id = body.get("cluster_id", "")
    # Update node cluster assignment
    node["cluster_id"] = cluster_id
    node["state"] = "joining"
    await reg.set(tenant, node_id, node)
    return {"node_id": node_id, "cluster_id": cluster_id, "state": "joining"}


@router.post("/{node_id}/drain", summary="Start draining node")
async def drain_node(
    node_id: str,
    request: Request,
    body: dict,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = resolve_tenant_id(_auth)
    node = await reg.get(tenant, node_id)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Node {node_id!r} not found")
    deadline_seconds = body.get("deadline_seconds", 300)
    node["state"] = "draining"
    await reg.set(tenant, node_id, node)
    return {"node_id": node_id, "state": "draining", "deadline_seconds": deadline_seconds}


@router.post("/{node_id}/release", summary="Release node from cluster")
async def release_node(
    node_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = resolve_tenant_id(_auth)
    node = await reg.get(tenant, node_id)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Node {node_id!r} not found")
    node["state"] = "idle"
    node["cluster_id"] = ""
    await reg.set(tenant, node_id, node)
    return {"node_id": node_id, "state": "idle"}


@router.delete("/{node_id}", status_code=204, summary="Unregister node")
async def delete_node(
    node_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> None:
    reg = _registry(request)
    tenant = resolve_tenant_id(_auth)
    await reg.delete(tenant, node_id)
