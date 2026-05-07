"""Redis HA client wrapper with retry, circuit breaker, and graceful degradation.
Simple implementation without external dependencies.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Optional

import redis.asyncio as aioredis
from redis.asyncio.sentinel import Sentinel

logger = logging.getLogger(__name__)


class CircuitBreaker:
    """Simple circuit breaker pattern."""
    
    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
    
    def record_failure(self) -> None:
        """Record a failure and potentially trip the breaker."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'
            logger.warning('Circuit breaker OPEN after %d failures', self.failure_count)
    
    def record_success(self) -> None:
        """Record a success and reset failure count."""
        if self.state == 'HALF_OPEN':
            self.state = 'CLOSED'
        self.failure_count = 0
        self.last_failure_time = None
    
    def should_allow(self) -> bool:
        """Check if request should be allowed."""
        if self.state == 'CLOSED':
            return True
        
        if self.state == 'OPEN':
            # Check if reset timeout has passed
            if self.last_failure_time and (time.time() - self.last_failure_time) > self.reset_timeout:
                self.state = 'HALF_OPEN'
                logger.info('Circuit breaker transitioning to HALF_OPEN')
                return True
            return False
        
        # HALF_OPEN: allow one request to test
        return True


class RedisHA:
    """High-availability Redis client with retry, circuit breaker, and degradation."""
    
    def __init__(
        self,
        redis_url: Optional[str] = None,
        sentinel_urls: Optional[list[str]] = None,
        master_name: str = 'mymaster',
        max_retries: int = 3,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 30.0,
        degradation_mode: bool = True,
    ):
        self.redis_url = redis_url
        self.sentinel_urls = sentinel_urls
        self.master_name = master_name
        self.max_retries = max_retries
        self.degradation_mode = degradation_mode
        
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_breaker_threshold,
            reset_timeout=circuit_breaker_timeout,
        )
        
        self._client: Optional[aioredis.Redis] = None
        self._sentinel: Optional[Sentinel] = None
        self._is_connected = False
        self._degradation_queue: list[tuple[str, tuple, dict]] = []  # (method, args, kwargs)
        
    async def start(self) -> None:
        """Initialize connection to Redis."""
        try:
            if self.sentinel_urls:
                # Sentinel mode
                self._sentinel = Sentinel(
                    self.sentinel_urls,
                    socket_timeout=0.5,
                    socket_connect_timeout=0.5,
                )
                self._client = self._sentinel.master_for(
                    self.master_name,
                    socket_timeout=0.5,
                    socket_connect_timeout=0.5,
                )
                logger.info('RedisHA connected via Sentinel to master=%s', self.master_name)
            else:
                # Standalone mode
                self._client = aioredis.from_url(
                    self.redis_url or 'redis://localhost:6379',
                    socket_timeout=0.5,
                    socket_connect_timeout=0.5,
                    retry_on_timeout=True,
                    max_connections=100,
                )
                logger.info('RedisHA connected to standalone Redis')
            
            # Test connection
            await self._client.ping()
            self._is_connected = True
            self.circuit_breaker.record_success()
            
        except Exception as e:
            logger.error('RedisHA connection failed: %s', e)
            self.circuit_breaker.record_failure()
            if not self.degradation_mode:
                raise
    
    async def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.aclose()
            self._client = None
            self._is_connected = False
            logger.info('RedisHA connection closed')
    
    def _should_degrade(self) -> bool:
        """Check if we should degrade operations."""
        return not self._is_connected or self.circuit_breaker.state == 'OPEN'
    
    async def _call_with_retry(self, method_name: str, *args, **kwargs) -> Any:
        """Call Redis method with retry and circuit breaker."""
        if not self.circuit_breaker.should_allow():
            raise ConnectionError('Circuit breaker is OPEN')
        
        if not self._client:
            raise ConnectionError('Redis client not initialized')
        
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                method = getattr(self._client, method_name)
                result = await method(*args, **kwargs)
                self.circuit_breaker.record_success()
                self._is_connected = True
                self._update_ha_metrics()
                return result
                
            except Exception as e:
                last_exception = e
                self.circuit_breaker.record_failure()
                logger.warning('RedisHA call failed (attempt %d/%d): %s - %s', 
                             attempt + 1, self.max_retries, method_name, str(e))
                
                # Record retry metric (best-effort)
                try:
                    from src.monitoring.metrics import REDIS_RETRY_TOTAL, REDIS_CIRCUIT_BREAKER_TRIPS
                    REDIS_RETRY_TOTAL.labels(operation=method_name).inc()
                    if self.circuit_breaker.state == 'OPEN' and attempt == 0:
                        REDIS_CIRCUIT_BREAKER_TRIPS.inc()
                except Exception:
                    pass  # Prometheus metrics are best-effort; never block the retry path
                
                if attempt < self.max_retries - 1:
                    # Exponential backoff with jitter: avoids thundering herd
                    # when multiple clients retry simultaneously after failures.
                    base = 0.1 * (2 ** attempt)
                    jitter = random.uniform(0, base * 0.2)
                    await asyncio.sleep(min(base + jitter, 30.0))
                    continue
                
                # After all retries failed
                self._is_connected = False
                self._update_ha_metrics()
                
                # In degradation mode, store failed writes for later retry
                if self.degradation_mode and method_name in ('set', 'setex', 'hset', 'zadd', 'sadd'):
                    self._degradation_queue.append((method_name, args, kwargs))
                    logger.info('Stored failed %s operation in degradation queue (size=%d)', 
                              method_name, len(self._degradation_queue))
                    # Return success for writes in degradation mode
                    if method_name == 'set':
                        return True
                    elif method_name == 'zadd':
                        return 1
                    elif method_name == 'sadd':
                        return 1
                    else:
                        return True
                
                raise last_exception
    
    def _update_ha_metrics(self) -> None:
        """Push Redis HA state to Prometheus (best-effort, never raises)."""
        try:
            from src.monitoring.metrics import update_redis_ha_metrics
            update_redis_ha_metrics(
                circuit_state=self.circuit_breaker.state,
                degradation_mode=not self._is_connected and self.degradation_mode,
                degradation_queue_size=len(self._degradation_queue),
            )
        except Exception:
            pass
    
    async def __call__(self, method_name: str, *args, **kwargs) -> Any:
        """Generic method call with HA features."""
        return await self._call_with_retry(method_name, *args, **kwargs)
    
    def __getattr__(self, name: str):
        """Delegate attribute access to the underlying client with HA wrapper."""
        if name.startswith('_'):
            return getattr(self, name)
        
        # Special handling for register_script
        if name == 'register_script':
            async def register_script_wrapper(script: str):
                """Register a Lua script and wrap its execution with retry logic."""
                if not self._client:
                    raise ConnectionError('Redis client not initialized')
                
                # Register the script with the underlying client
                script_obj = self._client.register_script(script)
                
                # Wrap the script call with our retry logic
                # script_obj.__call__ kept via register_script wrapping
                
                async def wrapped_script(keys=None, args=None, client=None):
                    # Convert keys/args to appropriate format for eval
                    keys_list = list(keys) if keys else []
                    args_list = list(args) if args else []
                    
                    # Call through our retry wrapper
                    return await self._call_with_retry('eval', script, 
                                                      len(keys_list),
                                                      *keys_list, *args_list)
                
                script_obj.__call__ = wrapped_script
                return script_obj
            
            return register_script_wrapper
        
        # For other methods, return a wrapper function
        async def wrapper(*args, **kwargs):
            return await self._call_with_retry(name, *args, **kwargs)
        
        return wrapper
    
    async def health_check(self) -> bool:
        """Check if Redis is healthy."""
        try:
            if self._client and self._is_connected:
                await self._client.ping()
                return True
        except Exception:
            pass
        return False
    
    async def retry_degraded_operations(self) -> int:
        """Retry operations stored during degradation mode."""
        if not self._degradation_queue or not self._is_connected:
            return 0
        
        retried = 0
        failed = []
        
        for method_name, args, kwargs in self._degradation_queue:
            try:
                await self._call_with_retry(method_name, *args, **kwargs)
                retried += 1
            except Exception:
                failed.append((method_name, args, kwargs))
        
        self._degradation_queue = failed
        logger.info('Retried %d degraded operations, %d still pending', 
                   retried, len(self._degradation_queue))
        return retried
