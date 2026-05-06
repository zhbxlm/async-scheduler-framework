"""Tests for RedisHA client."""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.common.redis_ha import RedisHA, CircuitBreaker


class TestCircuitBreaker:
    """Test CircuitBreaker functionality."""
    
    def test_initial_state(self):
        cb = CircuitBreaker(failure_threshold=3, reset_timeout=10)
        assert cb.state == 'CLOSED'
        assert cb.failure_count == 0
        assert cb.should_allow() == True
    
    def test_trip_to_open(self):
        cb = CircuitBreaker(failure_threshold=2, reset_timeout=10)
        cb.record_failure()
        assert cb.state == 'CLOSED'
        cb.record_failure()
        assert cb.state == 'OPEN'
        assert cb.should_allow() == False
    
    def test_reset_after_timeout(self):
        cb = CircuitBreaker(failure_threshold=1, reset_timeout=0.1)
        cb.record_failure()
        assert cb.state == 'OPEN'
        assert cb.should_allow() == False
        
        # Wait for timeout
        import time
        time.sleep(0.2)
        assert cb.should_allow() == True
        assert cb.state == 'HALF_OPEN'


@pytest.mark.asyncio
class TestRedisHA:
    """Test RedisHA client."""
    
    async def test_standalone_connection(self):
        """Test standalone Redis connection."""
        redis = RedisHA(redis_url='redis://localhost:6379')
        with patch('src.common.redis_ha.aioredis.from_url') as mock_from_url:
            mock_client = AsyncMock()
            mock_client.ping = AsyncMock()
            mock_from_url.return_value = mock_client
            
            await redis.start()
            assert redis._is_connected == True
            await redis.close()
    
    async def test_sentinel_connection(self):
        """Test Sentinel Redis connection."""
        redis = RedisHA(
            sentinel_urls=['redis://sentinel1:26379'],
            master_name='mymaster'
        )
        with patch('src.common.redis_ha.Sentinel') as mock_sentinel_class:
            mock_sentinel = MagicMock()
            mock_master = AsyncMock()
            mock_master.ping = AsyncMock()
            mock_sentinel.master_for.return_value = mock_master
            mock_sentinel_class.return_value = mock_sentinel
            
            await redis.start()
            assert redis._is_connected == True
            await redis.close()
    
    async def test_retry_on_failure(self):
        """Test retry logic on transient failures."""
        redis = RedisHA(redis_url='redis://localhost:6379')
        with patch('src.common.redis_ha.aioredis.from_url') as mock_from_url:
            mock_client = AsyncMock()
            mock_client.ping = AsyncMock()
            mock_client.get = AsyncMock(side_effect=[
                ConnectionError('First fail'),
                ConnectionError('Second fail'),
                'success_value'
            ])
            mock_from_url.return_value = mock_client
            
            await redis.start()
            
            # Should retry and eventually succeed
            result = await redis.get('test_key')
            assert result == 'success_value'
            assert mock_client.get.call_count == 3
            await redis.close()
    
    async def test_circuit_breaker_trip(self):
        """Test circuit breaker trips after repeated failures."""
        redis = RedisHA(
            redis_url='redis://localhost:6379',
            circuit_breaker_threshold=2,
            circuit_breaker_timeout=0.1
        )
        with patch('src.common.redis_ha.aioredis.from_url') as mock_from_url:
            mock_client = AsyncMock()
            mock_client.ping = AsyncMock()
            mock_client.get = AsyncMock(side_effect=ConnectionError('Always fails'))
            mock_from_url.return_value = mock_client
            
            await redis.start()
            
            # First call should fail but not trip
            try:
                await redis.get('test_key')
            except ConnectionError:
                pass
            
            # Second call should trip the breaker
            try:
                await redis.get('test_key')
            except ConnectionError as e:
                assert 'Circuit breaker is OPEN' in str(e)
            
            await redis.close()
    
    async def test_degradation_mode(self):
        """Test graceful degradation when Redis is unavailable."""
        redis = RedisHA(
            redis_url='redis://localhost:6379',
            degradation_mode=True
        )
        
        # Simulate connection failure
        with patch('src.common.redis_ha.aioredis.from_url') as mock_from_url:
            mock_client = AsyncMock()
            mock_client.ping = AsyncMock(side_effect=ConnectionError('Cannot connect'))
            # Make set fail to trigger degradation mode fallback
            mock_client.set = AsyncMock(side_effect=ConnectionError('Cannot connect'))
            mock_from_url.return_value = mock_client
            
            await redis.start()
            
            # In degradation mode, connection may still be marked as not connected
            # but the client should exist
            assert redis._client is not None
            
            # Write operations should 'succeed' in degradation mode after retries fail
            result = await redis.set('key', 'value')
            assert result == True  # Returns success in degradation mode
            
            await redis.close()
