from fastapi import FastAPI

from src.api.routes.ops import router


def test_ops_router_has_callback_and_timeline_routes():
    app = FastAPI()
    app.include_router(router)
    paths = {route.path for route in app.routes}
    assert "/ops/v1/dashboard/summary" in paths
    assert "/ops/v1/dashboard/stale-queue" in paths
    assert "/ops/v1/dashboard/replay-queue" in paths
    assert "/ops/v1/dashboard/dead-letter-backlog" in paths
    assert "/ops/v1/dashboard/recent-actions" in paths
    assert "/ops/v1/callbacks/summary" in paths
    assert "/ops/v1/callbacks/dead-letters" in paths
    assert "/ops/v1/callbacks/dead-letters/{outbox_id}/ack" in paths
    assert "/ops/v1/callbacks/dead-letters/{outbox_id}/replay" in paths
    assert "/ops/v1/tasks/{task_id}/timeline" in paths
    assert "/ops/v1/tasks/{task_id}/replay" in paths
    assert "/ops/v1/tasks/{task_id}/force-lease-eviction" in paths
    assert "/ops/v1/tasks/{task_id}/recovery-explanation" in paths
    assert "/ops/v1/tasks/{task_id}/runs" in paths
    assert "/ops/v1/tasks/{task_id}/runs/{run_key}/events" in paths
    assert "/ops/v1/dags/{dag_id}/runs" in paths
    assert "/ops/v1/operator-actions" in paths
