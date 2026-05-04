"""TaskCompletionNode — finalize tasks (persist + callback)."""
from __future__ import annotations
import httpx
import asyncio
from typing import Any


class TaskCompletionNode:
    def __init__(self):
        self._client = None
        self._client_lock = asyncio.Lock()

    async def handle_completion(self, task_id: str, status: str, result: Any) -> bool:
        """Persist to MySQL + send HTTP callback."""
        # TODO: implement
        return True

    async def _get_client(self) -> httpx.AsyncClient:
        async with self._client_lock:
            if self._client is None or self._client._loop != asyncio.get_event_loop():
                self._client = httpx.AsyncClient(timeout=30.0)
            return self._client
