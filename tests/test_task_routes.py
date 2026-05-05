"""Tests for the full task API routes (list/create/get/result/cancel)."""
from __future__ import annotations
import json
import pytest
from src.api.routes.tasks import _to_json, _serialize


# ---------------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------------

def test_to_json_from_dict():
    assert _to_json({"a": 1}) == {"a": 1}


def test_to_json_from_string():
    assert _to_json('{"x": 2}') == {"x": 2}


def test_to_json_none():
    assert _to_json(None) == {}


def test_to_json_bad_string():
    assert _to_json("not-json") == {}


def test_serialize_dict():
    s = _serialize({"k": "v"})
    assert '"k"' in s


def test_serialize_string():
    s = _serialize('{"k": "v"}')
    assert s == '{"k": "v"}'


# ---------------------------------------------------------------------------
# Integration tests — in-memory SQLite
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def task_client():
    """FastAPI TestClient with in-memory SQLite (StaticPool for module scope)."""
    from sqlalchemy import create_engine, StaticPool
    from sqlalchemy.orm import sessionmaker
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from src.common.async_db import Base
    from src.models import task as _task_models  # noqa: F401

    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,  # share single connection across threads/tests
    )
    Base.metadata.create_all(engine)
    TestSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    app = FastAPI()
    from src.api.routes.tasks import router
    app.include_router(router, prefix="/api/v1")

    from src.api.auth import authenticate
    from src.api.dependencies import get_tenant_context, get_db_session

    async def fake_auth():
        return {"tenant_id": "t1", "api_key": "key", "is_super_admin": False}

    class _FakeTenant:
        tenant_id = "t1"
        is_super_admin = False

    async def fake_tenant():
        return _FakeTenant()

    def fake_db():
        db = TestSession()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[authenticate] = fake_auth
    app.dependency_overrides[get_tenant_context] = fake_tenant
    app.dependency_overrides[get_db_session] = fake_db

    with TestClient(app) as client:
        yield client, TestSession


# ── tests ──────────────────────────────────────────────────────────────────

def test_list_tasks_empty(task_client):
    client, _ = task_client
    resp = client.get("/api/v1/tasks/")
    assert resp.status_code == 200
    assert "items" in resp.json()


def test_create_task(task_client):
    client, _, mock_task_creator = task_client
    resp = client.post("/api/v1/tasks/", json={
        "task_type": "video_gen",
        "input_data": {"file": "foo.mp4"},
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["task_id"].startswith("task-")
    assert data["status"] == "pending"


def test_get_task_detail(task_client):
    client, _ = task_client
    r = client.post("/api/v1/tasks/", json={"task_type": "get_test"})
    task_id = r.json()["task_id"]
    resp = client.get(f"/api/v1/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["task_id"] == task_id
    assert resp.json()["task_type"] == "get_test"


def test_get_task_not_found(task_client):
    client, _ = task_client
    assert client.get("/api/v1/tasks/no-such-task").status_code == 404


def test_result_not_ready(task_client):
    client, _ = task_client
    r = client.post("/api/v1/tasks/", json={"task_type": "wait_task"})
    task_id = r.json()["task_id"]
    assert client.get(f"/api/v1/tasks/{task_id}/result").status_code == 409


def test_result_available(task_client):
    from src.models.task import TaskRecord, TaskStatus
    client, TestSession = task_client
    r = client.post("/api/v1/tasks/", json={"task_type": "done_result"})
    task_id = r.json()["task_id"]

    with TestSession() as db:
        task = db.get(TaskRecord, task_id)
        task.status = TaskStatus.COMPLETED
        task.output_data = json.dumps({"result": "ok"})
        db.commit()

    resp = client.get(f"/api/v1/tasks/{task_id}/result")
    assert resp.status_code == 200
    assert resp.json()["status"] == "completed"
    assert resp.json()["output_data"] == {"result": "ok"}


def test_cancel_pending_task(task_client):
    client, _ = task_client
    r = client.post("/api/v1/tasks/", json={"task_type": "cancel_me"})
    task_id = r.json()["task_id"]
    resp = client.delete(f"/api/v1/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["cancelled"] is True
    assert resp.json()["prior_status"] == "pending"


def test_cancel_already_completed(task_client):
    from src.models.task import TaskRecord, TaskStatus
    client, TestSession = task_client
    r = client.post("/api/v1/tasks/", json={"task_type": "done_task"})
    task_id = r.json()["task_id"]

    with TestSession() as db:
        task = db.get(TaskRecord, task_id)
        task.status = TaskStatus.COMPLETED
        db.commit()

    resp = client.delete(f"/api/v1/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["cancelled"] is False


def test_idempotent_create(task_client):
    client, _ = task_client
    payload = {"task_type": "idem", "idempotency_key": "key-unique-xyz-123"}
    r1 = client.post("/api/v1/tasks/", json=payload)
    r2 = client.post("/api/v1/tasks/", json=payload)
    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r1.json()["task_id"] == r2.json()["task_id"]
    assert r2.json()["idempotent_reused"] is True


def test_list_tasks_after_create(task_client):
    client, _ = task_client
    client.post("/api/v1/tasks/", json={"task_type": "list_check_abc"})
    resp = client.get("/api/v1/tasks/")
    assert resp.status_code == 200
    assert any(i["task_type"] == "list_check_abc" for i in resp.json()["items"])


def test_list_tasks_status_filter(task_client):
    client, _ = task_client
    resp = client.get("/api/v1/tasks/?status=pending")
    assert resp.status_code == 200
    for item in resp.json()["items"]:
        assert item["status"] == "pending"
