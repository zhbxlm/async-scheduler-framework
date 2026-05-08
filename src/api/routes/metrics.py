"""Prometheus metrics endpoint for monitoring."""
from __future__ import annotations

from fastapi import APIRouter, Response
from fastapi.responses import PlainTextResponse

from src.monitoring.metrics import generate_metrics

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/", response_class=PlainTextResponse)
async def get_metrics() -> Response:
    """Expose Prometheus metrics."""
    metrics_data = generate_metrics()
    return Response(
        content=metrics_data,
        media_type="text/plain; version=0.0.4",
        headers={"Cache-Control": "no-cache"},
    )
