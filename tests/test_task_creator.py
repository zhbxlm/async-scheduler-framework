"""Unit tests for TaskCreator."""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

from src.platform.task_creator import TaskCreator


@pytest.fixture
def mock_redis():
    mock = AsyncMock()
    mock.set = AsyncMock(return_value=True)
    return mock


@pytest.fixture
def mock_queue_manager():
    mock = AsyncMock()
    mock.enqueue = AsyncMock(return_value={"accepted": True, "queue_position": 0, "pending_count": 1})
    return mock


@pytest.fixture
def task_creator(mock_redis, mock_queue_manager):
    return TaskCreator(
        redis_client=mock_redis,
        queue_manager=mock_queue_manager,
        db_session_factory=None,
    )


@pytest.mark.asyncio
async def test_create_task_basic(task_creator, mock_redis, mock_queue_manager):
    """Test basic task creation."""
    result = await task_creator.create_task(
        tenant_id="test-tenant",
        task_type="test_capability",
        priority="normal",
        input_data={"foo": "bar"},
    )
    
    assert "task_id" in result
    assert result["accepted"] is True
    assert result["queue_position"] == 0
    
    # Verify Redis storage
    mock_redis.set.assert_called_once()
    call_args = mock_redis.set.call_args
    assert call_args[0][0].startswith("task:")
    stored_json = call_args[0][1]
    import json
    stored = json.loads(stored_json)
    assert stored["task_type"] == "test_capability"
    assert stored["tenant_id"] == "test-tenant"
    assert stored["status"] == "queued"
    
    # Verify queue enqueue
    mock_queue_manager.enqueue.assert_called_once()
    enqueue_args = mock_queue_manager.enqueue.call_args
    assert enqueue_args.kwargs["capability"] == "test_capability"
    assert "task_id" in enqueue_args.kwargs


@pytest.mark.asyncio
async def test_create_task_with_idempotency_key(task_creator, mock_redis, mock_queue_manager):
    """Test task creation with idempotency key."""
    result = await task_creator.create_task(
        tenant_id="test-tenant",
        idempotency_key="schedule-123-456",
        task_type="cron_job",
    )
    
    assert "task_id" in result
    # Should start with idempotency prefix
    assert result["task_id"].startswith("cron-schedule-123-456-")
    
    mock_queue_manager.enqueue.assert_called_once()


@pytest.mark.asyncio
async def test_create_task_with_delay(task_creator, mock_redis, mock_queue_manager):
    """Test task creation with delay."""
    import time
    now_ms = int(time.time() * 1000)
    
    await task_creator.create_task(
        task_type="delayed_task",
        delay_seconds=60,
    )
    
    mock_queue_manager.enqueue.assert_called_once()
    enqueue_args = mock_queue_manager.enqueue.call_args
    # execute_after_ms should be ~60 seconds in future
    assert enqueue_args.kwargs["execute_after_ms"] > now_ms


@pytest.mark.asyncio
async def test_create_task_missing_capability(task_creator):
    """Test error when capability/task_type missing."""
    with pytest.raises(ValueError, match="task_type or capability must be provided"):
        await task_creator.create_task(tenant_id="test")


@pytest.mark.asyncio
async def test_create_task_with_db_persistence():
    """Test task creation with DB persistence (mock)."""
    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=True)
    mock_queue = AsyncMock()
    mock_queue.enqueue = AsyncMock(return_value={"accepted": True, "queue_position": 0, "pending_count": 1})
    
    # Mock DB session - use simpler mock to avoid async context issues
    mock_db_session = MagicMock()
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    mock_db_session.add = MagicMock()
    mock_db_session.commit = AsyncMock()
    
    mock_db_factory = MagicMock(return_value=mock_db_session)
    
    creator = TaskCreator(
        redis_client=mock_redis,
        queue_manager=mock_queue,
        db_session_factory=mock_db_factory,
    )
    
    result = await creator.create_task(
        tenant_id="db-tenant",
        task_type="persistent_task",
        priority="high",
        dispatch_mode="dag_orchestrated",
        input_data={"test": "data"},
        metadata={"source": "cron"},
        callback_url="http://example.com/callback",
        idempotency_key="key-123",
        timeout_seconds=7200,
        max_retries=5,
    )
    
    assert result["accepted"] is True
    # DB should have been called
    mock_db_factory.assert_called_once()
    mock_db_session.add.assert_called_once()
    mock_db_session.commit.assert_called_once()