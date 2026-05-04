"""WorkerDevKit — development tools for writing workers."""
from __future__ import annotations
import json
from typing import Any


class WorkerDevKit:
    def __init__(self):
        pass

    def mock_capability(self, name: str, handler: callable) -> None:
        """Register mock capability for local testing."""
        # TODO: implement
        pass

    def local_runner(self, task: dict) -> Any:
        """Run task locally (bypass queue)."""
        # TODO: implement
        return None
