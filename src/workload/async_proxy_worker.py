"""AsyncProxyWorker — aligned with docs/deepwiki-reference/异步代理.md

Plug-and-play Worker base class for long-running downstream services.
Encapsulates Proxy submit + Redis BLPOP result retrieval.

Subclass minimal contract:
    class MyWorker(AsyncProxyWorker):
        backend_path = "/generate"      # required

    # Optional overrides:
        def transform_input(self, input_data): ...
        def transform_output(self, data): ...
"""
from __future__ import annotations

import orjson
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class ProxySubmitError(Exception):
    """Proxy /submit call failed (network or Proxy service error)."""
    def __init__(self, detail: str, job_id: str = ""):
        super().__init__(detail)
        self.detail = detail
        self.job_id = job_id


class ProxyTimeoutError(Exception):
    """BLPOP wait exceeded timeout; downstream result not received."""
    def __init__(self, detail: str, job_id: str = ""):
        super().__init__(detail)
        self.detail = detail
        self.job_id = job_id


class ProxyBackendError(Exception):
    """Downstream algorithm service returned a business error."""
    def __init__(self, detail: str, job_id: str = "", http_status: int = 0):
        super().__init__(detail)
        self.detail = detail
        self.job_id = job_id
        self.http_status = http_status


# ---------------------------------------------------------------------------
# AsyncProxyWorker
# ---------------------------------------------------------------------------

class AsyncProxyWorker:
    """Worker base class that delegates execution to an AsyncServiceProxy.

    Class attributes to override:
      backend_path: str   — required, e.g. "/generate"
      timeout:      int   — BLPOP wait seconds (default 300)
      http_method:  str   — HTTP method forwarded to backend (default "POST")
    """

    backend_path: str = ""
    timeout: int = 300
    http_method: str = "POST"
    _default_options: dict = {"num_cpus": 1}

    def __init__(self, capability: str = "", config: dict | None = None) -> None:
        self.capability = capability
        self.config = config or {}
        self._call_count = 0
        self._actor_index: int = 0  # set by ActorPoolManager when using multi-backend

        # Resolve proxy URL(s)
        proxy_urls: list[str] = self.config.get("proxy_urls") or []
        proxy_url: str = self.config.get("proxy_url") or os.getenv("PROXY_URL", "")
        if proxy_urls:
            # Round-robin: actor picks its backend by index mod len
            self._proxy_url = proxy_urls[self._actor_index % len(proxy_urls)]
        elif proxy_url:
            self._proxy_url = proxy_url
        else:
            self._proxy_url = "http://localhost:5000"

        # Resolve Redis URL
        redis_url: str = self.config.get("redis_url") or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self._redis_url = redis_url

        # Override timeout from config
        if "timeout" in self.config:
            self.timeout = int(self.config["timeout"])

        self._redis = None      # lazy init
        self._http_client = None

    # ------------------------------------------------------------------
    # Template-method hooks
    # ------------------------------------------------------------------

    def pre_process(self, input_data: dict) -> dict:
        """Pre-processing hook (default: identity)."""
        return self.transform_input(input_data)

    def post_process(self, result: dict) -> dict:
        """Post-processing hook (default: identity)."""
        return result

    def transform_input(self, input_data: dict) -> Any:
        """Transform DAG input to backend payload format (default: passthrough)."""
        return input_data

    def transform_output(self, data: Any) -> dict:
        """Transform backend response to DAG output format (default: passthrough)."""
        return data if isinstance(data, dict) else {"result": data}

    # ------------------------------------------------------------------
    # Core run/call
    # ------------------------------------------------------------------

    def run(self, input_data: dict) -> dict:
        """Main entry point: pre_process → call → post_process."""
        processed = self.pre_process(input_data)
        raw_result = self.call(processed)
        result = self.post_process(raw_result)
        self._call_count += 1
        return result

    def call(self, data: Any) -> dict:
        """Submit to Proxy, BLPOP result from Redis, return transformed output.

        Raises:
            ProxySubmitError  — /submit failed
            ProxyTimeoutError — BLPOP timed out
            ProxyBackendError — backend returned error status
        """
        if not self.backend_path:
            raise ValueError("AsyncProxyWorker.backend_path must be set")

        # Build submit payload
        payload = {
            "_backend_path": self.backend_path,
            "_method": self.http_method,
            **(data if isinstance(data, dict) else {"data": data}),
        }

        # POST /submit
        submit_url = self._proxy_url.rstrip("/") + "/submit"
        try:
            import requests
            resp = requests.post(submit_url, json=payload, timeout=30)
            resp.raise_for_status()
            submit_resp = resp.json()
        except Exception as exc:
            raise ProxySubmitError(
                f"Failed to submit to {submit_url}: {exc}"
            ) from exc

        job_id     = submit_resp.get("job_id", "")
        result_key = submit_resp.get("result_key", "")
        if not result_key:
            raise ProxySubmitError("Proxy /submit did not return result_key", job_id=job_id)

        # BLPOP result
        r = self._get_redis()
        raw = r.blpop(result_key, timeout=self.timeout)
        if raw is None:
            raise ProxyTimeoutError(
                f"Timed out waiting for result_key={result_key} after {self.timeout}s",
                job_id=job_id,
            )

        _, result_raw = raw
        try:
            result = orjson.loads(result_raw)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ProxyBackendError(
                f"Invalid JSON in result: {exc}", job_id=job_id
            ) from exc

        status = result.get("status", "")
        if status == "cancelled":
            raise ProxyBackendError("Job was cancelled", job_id=job_id)
        if status == "error":
            raise ProxyBackendError(
                result.get("error") or str(result),
                job_id=job_id,
                http_status=result.get("http_status", 0),
            )

        # Extract business data (proxy wraps in {status, data, ...})
        business_data = result.get("data", result)
        return self.transform_output(business_data)

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    async def health_check(self) -> dict:
        """Check Proxy /health and Redis ping (async)."""
        proxy_ok = False
        redis_ok = False

        try:
            from src.common.http_client import async_get
            await async_get(self._proxy_url.rstrip("/") + "/health", timeout=5.0)
            proxy_ok = True
        except Exception:
            pass

        try:
            self._get_redis().ping()
            redis_ok = True
        except Exception:
            pass

        return {
            "status": "healthy" if (proxy_ok and redis_ok) else "unhealthy",
            "proxy_ok": proxy_ok,
            "redis_ok": redis_ok,
            "assigned_proxy": self._proxy_url,
            "processed_count": self._call_count,
            "capability": self.capability,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_redis(self):
        if self._redis is None:
            import redis as _redis
            self._redis = _redis.from_url(self._redis_url, decode_responses=False)
        return self._redis


# ---------------------------------------------------------------------------
# @ray.remote wrapper (optional)
# ---------------------------------------------------------------------------

try:
    import ray  # type: ignore[import]
    AsyncProxyWorker = ray.remote(AsyncProxyWorker)  # type: ignore[misc]
except ImportError:
    pass  # use plain Python class when Ray is not available
