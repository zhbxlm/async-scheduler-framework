from fastapi import FastAPI

from src.api.routes.ops import router


def test_ops_router_has_task_replay_route():
    app = FastAPI()
    app.include_router(router)
    paths = {route.path for route in app.routes}
    assert "/ops/v1/tasks/{task_id}/replay" in paths
