"""Tests for async proxy."""
import pytest
from src.proxy.async_command_proxy import AsyncCommandProxy


@pytest.mark.asyncio
async def test_async_proxy_basic():
    proxy = AsyncCommandProxy(endpoint="http://mock")
    result = await proxy.execute("echo", ["hello"])
    assert result["status"] == "proxy_not_implemented"
