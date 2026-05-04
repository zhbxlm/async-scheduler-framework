"""Unified error handling and logging utilities."""
from __future__ import annotations

import asyncio
import functools
import logging
import traceback
from contextlib import contextmanager
from typing import Any, Callable, TypeVar

from fastapi import HTTPException

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Error categories
# ---------------------------------------------------------------------------

class SystemError(Exception):
    """Internal system error (should be logged and fixed)."""
    pass


class BusinessError(Exception):
    """Business logic error (user input validation, constraints)."""
    pass


class ExternalServiceError(Exception):
    """External service failure (Redis, DB, third-party API)."""
    pass


# ---------------------------------------------------------------------------
# Decorators for error handling
# ---------------------------------------------------------------------------

F = TypeVar("F", bound=Callable[..., Any])


def log_errors(
    *,
    log_level: str = "ERROR",
    raise_exception: bool = True,
    exception_type: type[Exception] = SystemError,
) -> Callable[[F], F]:
    """Decorator to catch and log exceptions.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        raise_exception: Whether to re-raise the exception
        exception_type: Type to raise if raising a new exception
    """
    level = getattr(logging, log_level.upper())
    
    def decorator(func: F) -> F:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    return await func(*args, **kwargs)
                except (BusinessError, HTTPException):
                    raise  # Don't log business errors at ERROR level
                except Exception as exc:
                    logger.log(
                        level,
                        "Error in %s: %s\n%s",
                        func.__name__,
                        exc,
                        traceback.format_exc(),
                        extra={"function": func.__name__},
                    )
                    if raise_exception:
                        if not isinstance(exc, (BusinessError, HTTPException)):
                            raise exception_type(str(exc)) from exc
                        raise
                    return None
            return async_wrapper  # type: ignore
        else:
            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    return func(*args, **kwargs)
                except (BusinessError, HTTPException):
                    raise
                except Exception as exc:
                    logger.log(
                        level,
                        "Error in %s: %s\n%s",
                        func.__name__,
                        exc,
                        traceback.format_exc(),
                        extra={"function": func.__name__},
                    )
                    if raise_exception:
                        if not isinstance(exc, (BusinessError, HTTPException)):
                            raise exception_type(str(exc)) from exc
                        raise
                    return None
            return sync_wrapper  # type: ignore
    return decorator


@contextmanager
def suppress_errors(log_message: str | None = None):
    """Context manager to suppress and log errors."""
    try:
        yield
    except Exception as exc:
        if log_message:
            logger.warning("%s: %s", log_message, exc)
        else:
            logger.debug("Suppressed error: %s", exc)


# ---------------------------------------------------------------------------
# Structured logging helpers
# ---------------------------------------------------------------------------

def log_with_context(
    level: str,
    message: str,
    *args: Any,
    context: dict[str, Any] | None = None,
    **kwargs: Any,
) -> None:
    """Log with structured context."""
    log_func = getattr(logger, level.lower(), logger.info)
    extra = kwargs.pop("extra", {})
    if context:
        extra.update(context)
    
    log_func(message, *args, extra=extra if extra else None, **kwargs)


# ---------------------------------------------------------------------------
# FastAPI exception handlers (to be registered in main.py)
# ---------------------------------------------------------------------------

async def handle_system_error(request, exc: SystemError):
    from fastapi.responses import JSONResponse
    import logging
    logger = logging.getLogger(__name__)
    logger.error("System error: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "internal_server_error",
            "message": "An internal server error occurred",
            "request_id": request.state.get("request_id", ""),
        },
    )


async def handle_business_error(request, exc: BusinessError):
    from fastapi.responses import JSONResponse
    return JSONResponse(
        status_code=400,
        content={
            "error": "business_error",
            "message": str(exc),
            "request_id": request.state.get("request_id", ""),
        },
    )


async def handle_external_service_error(request, exc: ExternalServiceError):
    from fastapi.responses import JSONResponse
    import logging
    logger = logging.getLogger(__name__)
    logger.warning("External service error: %s", exc)
    return JSONResponse(
        status_code=503,
        content={
            "error": "service_unavailable",
            "message": "A dependent service is temporarily unavailable",
            "request_id": request.state.get("request_id", ""),
        },
    )