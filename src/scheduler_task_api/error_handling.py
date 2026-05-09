"""task-api package-owned error handling utilities."""
from __future__ import annotations

import asyncio
import functools
import logging
import traceback
from typing import Any, Callable, TypeVar

from fastapi import HTTPException

logger = logging.getLogger(__name__)
F = TypeVar("F", bound=Callable[..., Any])


class SystemError(Exception):
    pass


class BusinessError(Exception):
    pass


class ExternalServiceError(Exception):
    pass


def log_errors(*, log_level: str = "ERROR", raise_exception: bool = True, exception_type: type[Exception] = SystemError) -> Callable[[F], F]:
    level = getattr(logging, log_level.upper())

    def decorator(func: F) -> F:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    return await func(*args, **kwargs)
                except (BusinessError, HTTPException, ValueError):
                    raise
                except Exception as exc:
                    logger.log(level, "Error in %s: %s\n%s", func.__name__, exc, traceback.format_exc(), extra={"function": func.__name__})
                    if raise_exception:
                        if not isinstance(exc, (BusinessError, HTTPException)):
                            raise exception_type(str(exc)) from exc
                        raise
                    return None
            return async_wrapper  # type: ignore

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except (BusinessError, HTTPException, ValueError):
                raise
            except Exception as exc:
                logger.log(level, "Error in %s: %s\n%s", func.__name__, exc, traceback.format_exc(), extra={"function": func.__name__})
                if raise_exception:
                    if not isinstance(exc, (BusinessError, HTTPException)):
                        raise exception_type(str(exc)) from exc
                    raise
                return None
        return sync_wrapper  # type: ignore
    return decorator


async def handle_system_error(request, exc: SystemError):
    from fastapi.responses import JSONResponse
    logger.error("System error: %s", exc, exc_info=True)
    return JSONResponse(status_code=500, content={"error": "internal_server_error", "message": "An internal server error occurred", "request_id": getattr(request.state, "request_id", "")})


async def handle_business_error(request, exc: BusinessError):
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=400, content={"error": "business_error", "message": str(exc), "request_id": getattr(request.state, "request_id", "")})


async def handle_external_service_error(request, exc: ExternalServiceError):
    from fastapi.responses import JSONResponse
    logger.warning("External service error: %s", exc)
    return JSONResponse(status_code=503, content={"error": "service_unavailable", "message": "A dependent service is temporarily unavailable", "request_id": getattr(request.state, "request_id", "")})
