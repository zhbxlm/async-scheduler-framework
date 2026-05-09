import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_SRCS = [
    ROOT / "packages" / "runtime-core" / "src",
    ROOT / "packages" / "task-api" / "src",
]
for src in reversed(PACKAGE_SRCS):
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))

from scheduler_task_api.app import create_app


def test_task_api_package_app_imports_from_package_namespace():
    app = create_app()
    route_paths = {route.path for route in app.routes}
    assert "/api/v1/tasks/" in route_paths
    assert "/api/v1/health/" in route_paths
    assert "/metrics/" in route_paths
    assert app.router.lifespan_context is not None
