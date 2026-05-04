"""Tests for AsyncCommandProxy — updated for substantive implementation."""
import pytest
from unittest.mock import patch, MagicMock
from src.proxy.async_command_proxy import AsyncCommandProxy, _result_key


@pytest.mark.asyncio
async def test_async_proxy_submit_returns_job_id(monkeypatch):
    """submit() returns a job_id and result_key."""
    proxy = AsyncCommandProxy(endpoint="http://mock")
    resp = proxy.submit("echo hello")
    assert "job_id" in resp
    assert "result_key" in resp
    assert resp["result_key"] == _result_key(resp["job_id"])


@pytest.mark.asyncio
async def test_async_proxy_execute_timeout(monkeypatch):
    """execute() with no Redis returns timeout result."""
    proxy = AsyncCommandProxy(endpoint="http://mock")

    # Patch blpop_result to simulate timeout (returns None)
    monkeypatch.setattr(proxy, "blpop_result", lambda key, timeout=300: None)

    result = await proxy.execute("echo", ["hello"])
    assert result["status"] == "timeout"
