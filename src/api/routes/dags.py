"""dags routes — /ops/v1/dags
aligned with docs/deepwiki-reference/API 参考.md + DAG 编排.md
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from src.api.auth import authenticate
from src.api.dependencies import DagLoaderDep, RedisDep
from src.services.access_policy import resolve_tenant_id
from src.services.resource_application import DagService

router = APIRouter(prefix="/ops/v1/dags", tags=["dags"])


@router.get("/", summary="List registered DAGs")
async def list_dags(
    request: Request,
    loader: DagLoaderDep,
    redis: RedisDep,
    tenant_id: str = "default",
    _auth: dict = Depends(authenticate),
) -> list[dict]:
    """List DAG IDs via Redis scan (dag_def:{tenant_id}:* keys)."""
    tenant = resolve_tenant_id(_auth, tenant_id)
    return await DagService(loader, redis).list_dags(tenant)


@router.get("/{dag_id}", summary="Get DAG definition")
async def get_dag(
    dag_id: str,
    loader: DagLoaderDep,
    tenant_id: str = "default",
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth, tenant_id)
    return await DagService(loader).get_dag(dag_id, tenant)


@router.post("/", status_code=201, summary="Register DAG definition")
async def register_dag(
    body: dict,
    loader: DagLoaderDep,
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth)
    return await DagService(loader).register_dag(tenant, body)


@router.put("/{dag_id}", summary="Update DAG definition")
async def update_dag(
    dag_id: str,
    body: dict,
    loader: DagLoaderDep,
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth)
    return await DagService(loader).update_dag(dag_id, tenant, body)


@router.delete("/{dag_id}", status_code=204, summary="Delete DAG definition")
async def delete_dag(
    dag_id: str,
    loader: DagLoaderDep,
    tenant_id: str = "default",
    _auth: dict = Depends(authenticate),
) -> None:
    tenant = resolve_tenant_id(_auth, tenant_id)
    await DagService(loader).delete_dag(dag_id, tenant)
