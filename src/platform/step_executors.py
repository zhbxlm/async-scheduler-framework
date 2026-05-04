"""StepExecutors — sync/async/map step execution."""
from __future__ import annotations
import asyncio
from typing import Any


class SyncExecutor:
    def execute(self, step: dict, context: dict) -> Any:
        """Execute sync step."""
        # TODO: implement
        return None


class AsyncExecutor:
    async def execute(self, step: dict, context: dict) -> Any:
        """Execute async step."""
        # TODO: implement
        return None


class MapExecutor:
    async def execute(self, step: dict, context: dict) -> list[Any]:
        """Execute map step (parallel)."""
        # TODO: implement
        return []
