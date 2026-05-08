"""Shared async HTTP client pool.

Provides a single httpx.AsyncClient instance reused across the
application lifetime. Using a shared client:
  - Reuses TCP connections (connection pooling)
  - Reduces DNS resolution overhead
  - Respects per-host connection limits

Usage:
    from src.common.http_client import get_http_client

    client = get_http_client()
    resp = await client.post(url, json=payload)

Lifecycle:
    Call init_http_client() at app startup and close_http_client() at shutdown.
    The lifespan manager in container.py handles this automatically.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

_client: Optional[object] = None       # httpx.AsyncClient (app-level, managed)
_fallback_client: Optional[object] = None  # httpx.AsyncClient (test/script fallback)


def init_http_client(
    timeout: float = 30.0,
    max_connections: int = 100,
    max_keepalive_connections: int = 20,
) -> None:
    """Initialise the shared async HTTP client (call once at startup)."""
    global _client
    try:
        import httpx
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive_connections,
            ),
            follow_redirects=True,
        )
        logger.info(
            "HTTP client initialised: timeout=%.1fs max_conn=%d",
            timeout, max_connections,
        )
    except ImportError:
        logger.warning("httpx not installed; async HTTP client unavailable")


async def close_http_client() -> None:
    """Close the shared HTTP client and release connections."""
    global _client, _fallback_client
    if _client is not None:
        try:
            await _client.aclose()
            logger.info("HTTP client closed")
        except Exception as e:
            logger.warning("HTTP client close error: %s", e)
        finally:
            _client = None
    if _fallback_client is not None:
        try:
            await _fallback_client.aclose()
        except Exception:
            pass
        finally:
            _fallback_client = None


def get_http_client():
    """Return the shared httpx.AsyncClient.

    Falls back to a cached per-process client if not initialised (e.g. in tests).
    The fallback client is closed by close_http_client() at shutdown.
    """
    global _fallback_client
    if _client is not None:
        return _client
    # Lazy fallback for tests / scripts — cached so it isn't recreated every call
    try:
        import httpx
        if _fallback_client is None or _fallback_client.is_closed:
            _fallback_client = httpx.AsyncClient()
        return _fallback_client
    except ImportError:
        raise RuntimeError(
            "httpx not installed. Run: pip install httpx"
        )


async def async_post(url: str, payload: dict, *, timeout: float = 30.0) -> dict:
    """Convenience wrapper: POST JSON, return response dict.

    Replaces synchronous ``requests.post(url, json=payload)`` calls.
    """
    client = get_http_client()
    resp = await client.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


async def async_get(url: str, params: dict | None = None, *, timeout: float = 30.0) -> dict:
    """Convenience wrapper: GET with params, return response dict."""
    client = get_http_client()
    resp = await client.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()
