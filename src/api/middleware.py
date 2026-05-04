"""Request ID middleware.

Attaches a unique X-Request-ID header to every response, enabling:
  - Distributed tracing correlation
  - Log correlation across services
  - Support desk investigation

If the client sends X-Request-ID, it is echoed back; otherwise a new
UUID is generated. The ID is injected into the current trace span as
well (if tracing is enabled).
"""
from __future__ import annotations

import uuid
import time
import logging

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)

REQUEST_ID_HEADER = "X-Request-ID"
PROCESS_TIME_HEADER = "X-Process-Time-Ms"


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Adds X-Request-ID and X-Process-Time-Ms to all responses."""

    async def dispatch(self, request: Request, call_next) -> Response:
        # Respect client-provided ID or generate new
        req_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid.uuid4())

        # Inject into trace span
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

        # Structured access log
        logger.info(
            "method=%s path=%s status=%d request_id=%s duration_ms=%.2f",
            request.method,
            request.url.path,
            response.status_code,
            req_id,
            elapsed_ms,
        )

        return response
