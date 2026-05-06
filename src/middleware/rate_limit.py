"""Rate limiting middleware for FastAPI applications.

Supports Redis-based sliding window rate limiting per tenant/endpoint.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class RateLimiter:
    """Redis-based rate limiter using sliding window algorithm."""
    
    def __init__(self, redis_client: Any, key_prefix: str = "ratelimit"):
        self._redis = redis_client
        self._key_prefix = key_prefix
    
    async def is_allowed(
        self, 
        identifier: str, 
        window_seconds: int = 60,
        max_requests: int = 100
    ) -> tuple[bool, dict[str, Any]]:
        """Check if request is allowed within rate limits.
        
        Args:
            identifier: Unique identifier (e.g., "tenant:endpoint")
            window_seconds: Time window in seconds
            max_requests: Maximum requests allowed in window
            
        Returns:
            Tuple of (allowed, metadata)
        """
        try:
            key = f"{self._key_prefix}:{identifier}"
            now = int(time.time())
            
            # Remove old entries
            oldest = now - window_seconds
            await self._redis.zremrangebyscore(key, 0, oldest)
            
            # Count current requests in window
            current_count = await self._redis.zcard(key)
            
            if current_count >= max_requests:
                # Get oldest request time for retry-after calculation
                oldest_entry = await self._redis.zrange(key, 0, 0, withscores=True)
                retry_after = 0
                if oldest_entry:
                    retry_after = int(oldest_entry[0][1] + window_seconds - now)
                    retry_after = max(1, retry_after)
                
                return False, {
                    "limit": max_requests,
                    "remaining": 0,
                    "reset": now + retry_after,
                    "retry_after": retry_after,
                }
            
            # Add current request
            await self._redis.zadd(key, {str(now): now})
            await self._redis.expire(key, window_seconds)
            
            return True, {
                "limit": max_requests,
                "remaining": max_requests - current_count - 1,
                "reset": now + window_seconds,
                "retry_after": 0,
            }
            
        except Exception as e:
            # Fail open - allow request if Redis is unavailable
            logger.warning("Rate limiter failed: %s", e)
            return True, {"error": str(e), "fail_open": True}


class RateLimitMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware for rate limiting."""
    
    def __init__(
        self,
        app: FastAPI,
        redis_client: Any,
        default_window: int = 60,
        default_limit: int = 100,
        exempt_paths: list[str] | None = None,
    ):
        super().__init__(app)
        self.limiter = RateLimiter(redis_client)
        self.default_window = default_window
        self.default_limit = default_limit
        self.exempt_paths = exempt_paths or ["/health", "/docs", "/redoc", "/openapi.json"]
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Check if path is exempt
        if request.url.path in self.exempt_paths:
            return await call_next(request)
        
        # Extract tenant identifier
        tenant_id = "anonymous"
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            # Extract tenant from token (simplified)
            # In production, decode JWT to get tenant_id
            tenant_id = "authenticated"
        
        # Create rate limit key: tenant:method:path
        path = request.url.path.replace("/", "_")
        identifier = f"{tenant_id}:{request.method}:{path}"
        
        # Check rate limit
        allowed, metadata = await self.limiter.is_allowed(
            identifier,
            window_seconds=self.default_window,
            max_requests=self.default_limit,
        )
        
        if not allowed:
            headers = {
                "X-RateLimit-Limit": str(metadata["limit"]),
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(metadata["reset"]),
                "Retry-After": str(metadata["retry_after"]),
            }
            return JSONResponse(
                status_code=429,
                content={
                    "detail": f"Rate limit exceeded. Try again in {metadata['retry_after']} seconds.",
                    "retry_after": metadata["retry_after"],
                },
                headers=headers,
            )
        
        # Add rate limit headers to response
        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(metadata["limit"])
        response.headers["X-RateLimit-Remaining"] = str(metadata["remaining"])
        response.headers["X-RateLimit-Reset"] = str(metadata["reset"])
        
        return response