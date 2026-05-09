import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_SRCS = [
    ROOT / "packages" / "runtime-core" / "src",
    ROOT / "packages" / "agent" / "src",
]
for src in reversed(PACKAGE_SRCS):
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))

from scheduler_agent.node import NodeAgent
from scheduler_agent.server import create_app


def test_agent_package_server_exports_create_app():
    app = create_app(node_id="n1", host="127.0.0.1")
    route_paths = {route.path for route in app.routes}
    assert "/health" in route_paths
    assert "/invite" in route_paths
    assert "/node" in route_paths


def test_agent_public_node_class_still_package_owned():
    agent = NodeAgent()
    assert agent.__class__.__module__ == "scheduler_agent.node"
