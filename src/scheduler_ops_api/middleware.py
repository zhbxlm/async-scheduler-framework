"""task-api package-owned middleware."""
from __future__ import annotations

import logging
import time
import uuid
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.routing import Match

from src.monitoring.metrics import REQUEST_COUNT, REQUEST_LATENCY

logger = logging.getLogger(__name__)
REQUEST_ID_HEADER = "X-Request-ID"
PROCESS_TIME_HEADER = "X-Process-Time-Ms"


class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        req_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid.uuid4())
        try:
            from opentelemetry import trace
            span = trace.get_current_span()
            span.set_attribute("http.request_id", req_id)
        except Exception:
            pass
        t0 = time.perf_counter()
        response: Response = await call_next(request)
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        response.headers[REQUEST_ID_HEADER] = req_id
        response.headers[PROCESS_TIME_HEADER] = str(elapsed_ms)
        logger.info("method=%s path=%s status=%d request_id=%s duration_ms=%.2f", request.method, request.url.path, response.status_code, req_id, elapsed_ms)
        return response


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if request.url.path == "/metrics":
            return await call_next(request)
        endpoint = "unknown"
        for route in request.app.routes:
            match, _ = route.matches(request.scope)
            if match == Match.FULL:
                endpoint = route.path
                break
        method = request.method
        start_time = time.time()
        try:
            response = await call_next(request)
            status_code = str(response.status_code)
        except Exception:
            status_code = "500"
            raise
        finally:
            duration = time.time() - start_time
            REQUEST_LATENCY.labels(method=method, endpoint=endpoint).observe(duration)
            REQUEST_COUNT.labels(method=method, endpoint=endpoint, status_code=status_code).inc()
        return response
