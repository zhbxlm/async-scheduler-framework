"""Callback dispatch stub with retry bookkeeping."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


class CallbackDispatcher:
    """Dispatches task completion callbacks.

    Current implementation is a local stub so the framework can evolve without
    requiring outbound network access in dev environments.
    """

    def __init__(self, max_attempts: int = 3, retry_delay_seconds: float = 1.0) -> None:
        self.max_attempts = max_attempts
        self.retry_delay_seconds = retry_delay_seconds

    async def dispatch(self, callback_url: str | None, payload: dict[str, Any]) -> bool:
        if not callback_url:
            return True

        for attempt in range(1, self.max_attempts + 1):
            try:
                logger.info("callback dispatch attempt=%s url=%s payload=%s", attempt, callback_url, payload)
                await asyncio.sleep(0)
                return True
            except Exception:
                logger.exception("callback dispatch failed attempt=%s url=%s", attempt, callback_url)
                if attempt < self.max_attempts:
                    await asyncio.sleep(self.retry_delay_seconds)
        return False
