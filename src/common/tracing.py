"""OpenTelemetry tracing setup and utilities.

Provides distributed tracing via OTLP exporter or console (dev mode).
Usage:
    from src.common.tracing import tracer, setup_tracing
    
    setup_tracing(service_name="scheduler-api")
    
    with tracer.start_as_current_span("my_operation") as span:
        span.set_attribute("task.id", task_id)
        ...
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Generator, Optional, Any

logger = logging.getLogger(__name__)

# Lazy imports to avoid hard dependency
_tracer = None
_tracer_provider = None


def setup_tracing(
    service_name: str = "async-scheduler",
    service_version: str = "1.0.0",
    environment: str = "development",
    otlp_endpoint: Optional[str] = None,
    console_export: bool = False,
) -> None:
    """Configure OpenTelemetry tracing.

    Args:
        service_name: Service name in trace metadata.
        service_version: Service version.
        environment: deployment environment (development/production).
        otlp_endpoint: OTLP gRPC endpoint (e.g., "http://jaeger:4317").
                       Falls back to OTEL_EXPORTER_OTLP_ENDPOINT env var.
        console_export: Also export spans to stdout (useful in dev).
    """
    global _tracer, _tracer_provider

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.semconv.resource import ResourceAttributes

        resource = Resource.create({
            ResourceAttributes.SERVICE_NAME: service_name,
            ResourceAttributes.SERVICE_VERSION: service_version,
            ResourceAttributes.DEPLOYMENT_ENVIRONMENT: environment,
        })

        _tracer_provider = TracerProvider(resource=resource)

        # OTLP exporter (Jaeger / Tempo / etc.)
        endpoint = otlp_endpoint or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
        if endpoint:
            try:
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
                otlp_exporter = OTLPSpanExporter(endpoint=endpoint)
                _tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
                logger.info("Tracing: OTLP exporter → %s", endpoint)
            except Exception as e:
                logger.warning("Tracing: OTLP exporter failed: %s", e)

        # Console exporter (dev/debug)
        if console_export or environment == "development":
            _tracer_provider.add_span_processor(
                BatchSpanProcessor(ConsoleSpanExporter())
            )

        trace.set_tracer_provider(_tracer_provider)
        _tracer = trace.get_tracer(service_name, service_version)
        logger.info("Tracing initialised: service=%s env=%s", service_name, environment)

    except ImportError:
        logger.warning("opentelemetry not installed; tracing disabled")
        _tracer = _NoopTracer()


def instrument_fastapi(app: Any) -> None:
    """Instrument a FastAPI app with automatic span creation per request."""
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor.instrument_app(app)
        logger.info("Tracing: FastAPI instrumented")
    except ImportError:
        logger.debug("FastAPIInstrumentor not available; skipping")


def instrument_redis(client: Any) -> None:
    """Instrument a Redis client to trace all commands."""
    try:
        from opentelemetry.instrumentation.redis import RedisInstrumentor
        RedisInstrumentor().instrument()
        logger.info("Tracing: Redis instrumented")
    except ImportError:
        logger.debug("RedisInstrumentor not available; skipping")


def get_tracer():
    """Get the global tracer (noop if tracing not initialised)."""
    global _tracer
    if _tracer is None:
        _tracer = _NoopTracer()
    return _tracer


@contextmanager
def trace_span(
    name: str,
    attributes: Optional[dict] = None,
) -> Generator:
    """Context manager for manual span creation.

    Usage:
        with trace_span("process_task", {"task.id": task_id}) as span:
            span.set_attribute("task.priority", priority)
            ...
    """
    tracer = get_tracer()
    with tracer.start_as_current_span(name) as span:
        if attributes:
            for k, v in attributes.items():
                try:
                    span.set_attribute(k, v)
                except Exception:
                    pass
        yield span


def trace_method(span_name: Optional[str] = None, attributes: Optional[dict] = None):
    """Decorator to trace a function/method automatically.

    Usage:
        @trace_method("registry.get")
        async def get(self, tenant_id, item_id):
            ...
    """
    import functools

    def decorator(fn):
        name = span_name or f"{fn.__module__}.{fn.__qualname__}"

        if __import__("asyncio").iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def async_wrapper(*args, **kwargs):
                with trace_span(name, attributes) as span:
                    try:
                        result = await fn(*args, **kwargs)
                        return result
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(
                            __import__("opentelemetry.trace", fromlist=["StatusCode"]).StatusCode.ERROR,
                            str(e),
                        )
                        raise
            return async_wrapper
        else:
            @functools.wraps(fn)
            def sync_wrapper(*args, **kwargs):
                with trace_span(name, attributes) as span:
                    try:
                        return fn(*args, **kwargs)
                    except Exception as e:
                        span.record_exception(e)
                        raise
            return sync_wrapper

    return decorator


def shutdown_tracing() -> None:
    """Flush and shutdown tracer provider."""
    global _tracer_provider  # noqa: F824  # noqa: F824
    if _tracer_provider is not None:
        try:
            _tracer_provider.shutdown()
            logger.info("Tracing: provider shut down")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Noop fallback (when opentelemetry is not installed)
# ---------------------------------------------------------------------------

class _NoopSpan:
    def set_attribute(self, *a, **kw): pass
    def record_exception(self, *a, **kw): pass
    def set_status(self, *a, **kw): pass
    def __enter__(self): return self
    def __exit__(self, *a): pass


class _NoopTracer:
    @contextmanager
    def start_as_current_span(self, name: str, **kw) -> Generator:
        yield _NoopSpan()
