from fastapi import FastAPI

from src.api.routes.ops import router


def test_ops_router_has_callback_and_timeline_routes():
    app = FastAPI()
    app.include_router(router)
    paths = {route.path for route in app.routes}
    assert "/ops/v1/callbacks/summary" in paths
    assert "/ops/v1/callbacks/dead-letters" in paths
    assert "/ops/v1/callbacks/dead-letters/{outbox_id}/ack" in paths
    assert "/ops/v1/callbacks/dead-letters/{outbox_id}/replay" in paths
    assert "/ops/v1/tasks/{task_id}/timeline" in paths
    assert "/ops/v1/tasks/{task_id}/replay" in paths
    assert "/ops/v1/operator-actions" in paths
