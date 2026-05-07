"""HTTP client for CLI."""
import os
import random
import time
from typing import Any

import click
import httpx


class ApiClient:
    def __init__(self, base_url: str, api_key: str = ""):
        self._base = base_url.rstrip("/")
        self._key = api_key or os.getenv("RAY_ASYNC_API_KEY", "")
        self._client = httpx.Client(timeout=30.0)

    def get(self, path: str, **kwargs) -> Any:
        return self._request("GET", path, **kwargs)

    def post(self, path: str, json: dict, **kwargs) -> Any:
        return self._request("POST", path, json=json, **kwargs)

    def put(self, path: str, json: dict, **kwargs) -> Any:
        return self._request("PUT", path, json=json, **kwargs)

    def delete(self, path: str, **kwargs) -> Any:
        return self._request("DELETE", path, **kwargs)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        """Send HTTP request with retry and backoff logic."""
        url = f"{self._base}{path}"
        headers = kwargs.pop("headers", {})
        if self._key:
            headers["Authorization"] = f"Bearer {self._key}"
        
        # Retry configuration
        max_retries = 3
        base_delay = 1.0  # seconds
        max_delay = 10.0  # seconds
        
        last_exception = None
        
        for attempt in range(max_retries + 1):  # +1 for initial attempt
            try:
                resp = self._client.request(method, url, headers=headers, **kwargs)
                resp.raise_for_status()
                
                # Success - return response
                if resp.content:
                    return resp.json()
                return None
                
            except httpx.HTTPStatusError as e:
                # Don't retry on 4xx client errors (except 429 rate limit)
                status_code = e.response.status_code
                if 400 <= status_code < 500 and status_code != 429:
                    detail = ""
                    try:
                        detail = e.response.text[:200]
                    except Exception:
                        pass
                    msg = f"{method} {path} failed: {status_code}"
                    if detail:
                        msg += f" — {detail}"
                    raise click.ClickException(msg)
                
                last_exception = e
                
            except httpx.RequestError as e:
                # Network errors - retry with backoff
                last_exception = e
                
            # Calculate backoff delay (exponential with jitter)
            if attempt < max_retries:
                delay = min(base_delay * (2 ** attempt), max_delay)
                jitter = random.uniform(0, delay * 0.2)  # ±20% jitter
                delay = delay + jitter
                
                click.echo(
                    f"Request failed (attempt {attempt + 1}/{max_retries + 1}), "
                    f"retrying in {delay:.1f}s...",
                    err=True
                )
                time.sleep(delay)
            
        # All retries exhausted
        if isinstance(last_exception, httpx.HTTPStatusError):
            detail = ""
            try:
                detail = last_exception.response.text[:200]
            except Exception:
                pass
            msg = f"{method} {path} failed after {max_retries + 1} attempts: {last_exception.response.status_code}"
            if detail:
                msg += f" — {detail}"
            raise click.ClickException(msg)
        else:
            raise click.ClickException(
                f"Cannot reach API at {self._base} after {max_retries + 1} attempts: {last_exception}"
            )
        return None
