import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_SRCS = [
    ROOT / "packages" / "runtime-core" / "src",
    ROOT / "packages" / "control-plane" / "src",
]
for src in reversed(PACKAGE_SRCS):
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))

from scheduler_control_plane.app import create_runtime


def test_control_plane_package_runtime_imports_from_package_namespace():
    runtime = create_runtime()
    assert callable(runtime)
    assert runtime.__module__ == "scheduler_control_plane.runtime"
