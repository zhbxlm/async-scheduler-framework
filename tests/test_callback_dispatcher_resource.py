from unittest.mock import AsyncMock, MagicMock

import pytest
from src.common.lifecycle import CallbackDispatcherResource


def test_callback_dispatcher_resource_name():
    resource = CallbackDispatcherResource(MagicMock())
    assert resource.name == "CallbackDispatcher"


@pytest.mark.asyncio
async def test_callback_dispatcher_resource_delegates_start():
    svc = MagicMock()
    svc.start = AsyncMock()
    resource = CallbackDispatcherResource(svc)
    await resource.start()
    svc.start.assert_awaited_once()
