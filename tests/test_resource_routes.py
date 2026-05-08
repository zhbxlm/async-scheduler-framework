from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.routes.clusters import router as clusters_router
from src.api.routes.dags import router as dags_router
from src.api.routes.nodes import router as nodes_router
from src.api.routes.schedules import router as schedules_router


class _AuthBypassMiddleware:
    pass


class FakeRegistry:
    def __init__(self, initial: dict[str, dict[str, dict]] | None = None):
        self.data = initial or {}

    async def list(self, tenant_id: str):
        return list(self.data.get(tenant_id, {}).keys())

    async def get(self, tenant_id: str, item_id: str):
        return self.data.get(tenant_id, {}).get(item_id)

    async def set(self, tenant_id: str, item_id: str, value: dict):
        self.data.setdefault(tenant_id, {})[item_id] = value
        return True

    async def delete(self, tenant_id: str, item_id: str):
        self.data.setdefault(tenant_id, {}).pop(item_id, None)
        return True

    async def toggle(self, tenant_id: str, item_id: str, enabled: bool):
        item = self.data.get(tenant_id, {}).get(item_id)
        if item is None:
            return False
        item["enabled"] = enabled
        return True


class FakeDagLoader:
    def __init__(self, initial: dict[tuple[str, str], dict] | None = None):
        self.data = initial or {}

    async def load(self, dag_id: str, tenant_id: str = "default"):
        return self.data.get((tenant_id, dag_id))

    async def register(self, dag_id: str, tenant_id: str, definition: dict):
        self.data[(tenant_id, dag_id)] = definition
        return True

    async def delete(self, dag_id: str, tenant_id: str):
        self.data.pop((tenant_id, dag_id), None)
        return True


class FakeRedisScan:
    def __init__(self, keys: list[str]):
        self.keys = keys

    async def scan_iter(self, pattern: str):
        prefix = pattern[:-1] if pattern.endswith("*") else pattern
        for key in self.keys:
            if key.startswith(prefix):
                yield key


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(clusters_router)
    app.include_router(nodes_router)
    app.include_router(dags_router)
    app.include_router(schedules_router)

    async def _fake_auth():
        return {"tenant_id": "tenant-a", "api_key": "test", "is_super_admin": False}

    from src.api.auth import authenticate

    app.dependency_overrides[authenticate] = _fake_auth
    return app


def test_cluster_routes_delegate_through_service_layer():
    app = _build_app()
    app.state.cluster_registry = FakeRegistry(
        {"tenant-a": {"c1": {"cluster_id": "c1", "planned_resources": {"gpu": 1}, "observed_resources": {"gpu": 0}}}}
    )

    client = TestClient(app)

    resp = client.get("/ops/v1/clusters/")
    assert resp.status_code == 200
    assert resp.json()[0]["cluster_id"] == "c1"

    resp = client.get("/ops/v1/clusters/c1/resources")
    assert resp.status_code == 200
    body = resp.json()
    assert body["cluster_id"] == "c1"
    assert body["planned"] == {"gpu": 1}
    assert body["observed"] == {"gpu": 0}

    resp = client.post("/ops/v1/clusters/", json={"cluster_id": "c2", "name": "cluster-2"})
    assert resp.status_code == 201
    assert app.state.cluster_registry.data["tenant-a"]["c2"]["name"] == "cluster-2"


def test_node_routes_support_invite_drain_release():
    app = _build_app()
    app.state.node_registry = FakeRegistry(
        {"tenant-a": {"n1": {"node_id": "n1", "cluster_id": "", "state": "idle"}}}
    )

    client = TestClient(app)

    resp = client.post("/ops/v1/nodes/n1/invite", json={"cluster_id": "c1"})
    assert resp.status_code == 200
    assert resp.json() == {"node_id": "n1", "cluster_id": "c1", "state": "joining"}

    resp = client.post("/ops/v1/nodes/n1/drain", json={"deadline_seconds": 120})
    assert resp.status_code == 200
    assert resp.json()["deadline_seconds"] == 120

    resp = client.post("/ops/v1/nodes/n1/release")
    assert resp.status_code == 200
    assert resp.json() == {"node_id": "n1", "state": "idle"}


def test_dag_routes_preserve_listing_and_update_contract():
    app = _build_app()
    app.state.dag_loader = FakeDagLoader({("tenant-a", "d1"): {"dag_id": "d1", "version": 1}})
    app.state.redis = FakeRedisScan(["dag_def:tenant-a:d1", "dag_def:tenant-a:d2", "dag_def:tenant-b:other"])

    client = TestClient(app)

    resp = client.get("/ops/v1/dags/")
    assert resp.status_code == 200
    assert resp.json() == [
        {"dag_id": "d1", "tenant_id": "tenant-a"},
        {"dag_id": "d2", "tenant_id": "tenant-a"},
    ]

    resp = client.put("/ops/v1/dags/d1", json={"version": 2})
    assert resp.status_code == 200
    assert resp.json()["dag_id"] == "d1"
    assert resp.json()["version"] == 2


def test_schedule_routes_create_toggle_and_list():
    app = _build_app()
    app.state.schedule_registry = FakeRegistry(
        {"tenant-a": {"s1": {"schedule_id": "s1", "cron_expr": "* * * * *", "enabled": True}}}
    )

    client = TestClient(app)

    resp = client.get("/ops/v1/schedules/")
    assert resp.status_code == 200
    assert resp.json()[0]["schedule_id"] == "s1"

    resp = client.post("/ops/v1/schedules/", json={"schedule_id": "s2", "cron_expr": "*/5 * * * *"})
    assert resp.status_code == 201
    assert resp.json()["tenant_id"] == "tenant-a"
    assert resp.json()["enabled"] is True

    resp = client.post("/ops/v1/schedules/s2/toggle", json={"enabled": False})
    assert resp.status_code == 200
    assert resp.json() == {"schedule_id": "s2", "enabled": False}
