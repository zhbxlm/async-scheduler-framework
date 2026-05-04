"""Tests for SchedulerClient SDK."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_client_submit_task():
    """Test task submission via SDK."""
    from src.sdk.client import SchedulerClient

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "task_id": "test-123",
        "status": "pending",
        "dispatch_mode": "dag_orchestrated",
    }
    mock_resp.raise_for_status = MagicMock()

    async with SchedulerClient("http://localhost:8000", api_key="test-key") as client:
        client._client = AsyncMock()
        client._client.post = AsyncMock(return_value=mock_resp)

        result = await client.submit_task(
            dag_id="train_v1",
            capability="gpu_training",
            input_data={"batch": 32},
        )

    assert result["task_id"] == "test-123"
    assert result["status"] == "pending"


@pytest.mark.asyncio
async def test_client_wait_for_task_success():
    """Test wait_for_task returns on completed status."""
    from src.sdk.client import SchedulerClient
    import asyncio

    completed_resp = MagicMock()
    completed_resp.status_code = 200
    completed_resp.json.return_value = {"task_id": "t1", "status": "completed", "output_data": {"score": 0.95}}
    completed_resp.raise_for_status = MagicMock()

    async with SchedulerClient("http://localhost:8000") as client:
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=completed_resp)

        result = await client.wait_for_task("t1", poll_interval=0.01, timeout=5.0)

    assert result["status"] == "completed"
    assert result["output_data"]["score"] == 0.95


@pytest.mark.asyncio
async def test_client_wait_for_task_failed():
    """Test wait_for_task raises on failed status."""
    from src.sdk.client import SchedulerClient

    failed_resp = MagicMock()
    failed_resp.status_code = 200
    failed_resp.json.return_value = {"task_id": "t2", "status": "failed", "output_data": {}}
    failed_resp.raise_for_status = MagicMock()

    async with SchedulerClient("http://localhost:8000") as client:
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=failed_resp)

        with pytest.raises(RuntimeError, match="failed"):
            await client.wait_for_task("t2", poll_interval=0.01, timeout=5.0)


@pytest.mark.asyncio
async def test_client_wait_for_task_timeout():
    """Test wait_for_task raises TimeoutError."""
    from src.sdk.client import SchedulerClient

    pending_resp = MagicMock()
    pending_resp.status_code = 200
    pending_resp.json.return_value = {"task_id": "t3", "status": "running"}
    pending_resp.raise_for_status = MagicMock()

    async with SchedulerClient("http://localhost:8000") as client:
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=pending_resp)

        with pytest.raises(TimeoutError):
            await client.wait_for_task("t3", poll_interval=0.01, timeout=0.05)


@pytest.mark.asyncio
async def test_client_is_ready_true():
    """Test is_ready returns True on 200."""
    from src.sdk.client import SchedulerClient

    resp = MagicMock()
    resp.status_code = 200

    async with SchedulerClient("http://localhost:8000") as client:
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=resp)
        result = await client.is_ready()

    assert result is True


@pytest.mark.asyncio
async def test_client_is_ready_false_on_error():
    """Test is_ready returns False on connection error."""
    from src.sdk.client import SchedulerClient

    async with SchedulerClient("http://localhost:8000") as client:
        client._client = AsyncMock()
        client._client.get = AsyncMock(side_effect=Exception("refused"))
        result = await client.is_ready()

    assert result is False
