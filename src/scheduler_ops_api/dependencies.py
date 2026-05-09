"""ops-api package-owned FastAPI dependencies."""
from __future__ import annotations

from typing import Annotated, Any

from fastapi import Depends, HTTPException, Request


def _get_app_state_attr(request: Request, attr_name: str, detail: str) -> Any:
    value = getattr(request.app.state, attr_name, None)
    if value is None:
        raise HTTPException(status_code=503, detail=detail)
    return value


def get_cluster_registry(request: Request):
    return _get_app_state_attr(request, "cluster_registry", "ClusterRegistry not initialised")


def get_node_registry(request: Request):
    return _get_app_state_attr(request, "node_registry", "NodeRegistry not initialised")


def get_schedule_registry(request: Request):
    return _get_app_state_attr(request, "schedule_registry", "ScheduleRegistry not initialised")


def get_dag_loader(request: Request):
    return _get_app_state_attr(request, "dag_loader", "DagLoader not initialised")


def get_redis(request: Request):
    return getattr(request.app.state, "redis", None)


ClusterRegistryDep = Annotated[Any, Depends(get_cluster_registry)]
NodeRegistryDep = Annotated[Any, Depends(get_node_registry)]
ScheduleRegistryDep = Annotated[Any, Depends(get_schedule_registry)]
DagLoaderDep = Annotated[Any, Depends(get_dag_loader)]
RedisDep = Annotated[Any | None, Depends(get_redis)]
