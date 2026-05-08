from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.routes.health import router


def test_health_ready_endpoint_exists():
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    resp = client.get("/health/ready")
    assert resp.status_code == 200
    data = resp.json()
    assert "status" in data
    assert "checks" in data


def test_health_platform_endpoint_exists():
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    resp = client.get("/health/platform")
    assert resp.status_code == 200
