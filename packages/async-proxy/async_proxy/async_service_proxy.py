"""AsyncServiceProxy — Sidecar HTTP proxy for long-running services.

Receives submit requests, forwards to backend asynchronously in thread pool,
writes results to Redis so worker can BLPOP and retrieve them.
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import redis as redis_sync
import requests
from flask import Flask, jsonify, request

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config (env overrides)
# ---------------------------------------------------------------------------
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8080")
BACKEND_TIMEOUT = int(os.getenv("BACKEND_TIMEOUT", "600"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RESULT_TTL = int(os.getenv("RESULT_TTL", "3600"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "32"))
PROXY_PORT = int(os.getenv("PROXY_PORT", "5000"))

# Hop-by-hop headers to strip when forwarding
_HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
})


def _get_redis() -> redis_sync.Redis:
    """Get sync Redis client."""
    return redis_sync.from_url(REDIS_URL, decode_responses=False)


def _forward_and_store(
    job_id: str,
    result_key: str,
    backend_url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    method: str,
    redis_client: redis_sync.Redis,
) -> None:
    """Background thread: forward request → store result in Redis."""
    t0 = time.time()
    try:
        if method.upper() == "GET":
            resp = requests.get(backend_url, params=payload, headers=headers,
                                timeout=BACKEND_TIMEOUT)
        else:
            resp = requests.post(backend_url, json=payload, headers=headers,
                                 timeout=BACKEND_TIMEOUT)

        elapsed = time.time() - t0
        try:
            data = resp.json()
        except Exception:
            data = resp.text

        result = {
            "status": "success" if resp.ok else "error",
            "http_status": resp.status_code,
            "data": data,
            "elapsed": round(elapsed, 3),
        }
    except Exception as exc:
        result = {
            "status": "error",
            "http_status": 0,
            "data": str(exc),
            "elapsed": round(time.time() - t0, 3),
        }

    redis_client.lpush(result_key, json.dumps(result))
    redis_client.expire(result_key, RESULT_TTL)


def create_proxy_app(
    backend_url: str = BACKEND_URL,
    redis_url: str = REDIS_URL,
) -> Flask:
    """Create and return the AsyncServiceProxy Flask app.

    Parameters
    ----------
    backend_url : str
        URL of the backend service to proxy to.
    redis_url : str
        Redis connection URL for storing results.

    Returns
    -------
    Flask
        The Flask application.
    """
    app = Flask(__name__)
    pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    _redis = redis_sync.from_url(redis_url, decode_responses=False)

    @app.route("/submit", methods=["POST"])
    def submit():
        """Submit a job for async execution."""
        body: dict = request.get_json(force=True) or {}
        backend_path = body.pop("_backend_path", "/generate")
        method = body.pop("_method", "POST")
        job_id = str(uuid.uuid4())
        result_key = f"proxy_result:{job_id}"

        # Filter hop-by-hop headers
        fwd_headers = {
            k: v for k, v in request.headers.items()
            if k.lower() not in _HOP_BY_HOP and k.lower() != "host"
        }

        full_url = backend_url.rstrip("/") + "/" + backend_path.lstrip("/")
        pool.submit(
            _forward_and_store,
            job_id, result_key, full_url, body, fwd_headers, method, _redis,
        )
        return jsonify({"job_id": job_id, "result_key": result_key}), 202

    @app.route("/result/<job_id>", methods=["GET"])
    def get_result(job_id: str):
        """Get job result."""
        result_key = f"proxy_result:{job_id}"
        raw = _redis.lindex(result_key, 0)
        if raw is None:
            return jsonify({"status": "pending"}), 202
        return jsonify(json.loads(raw)), 200

    @app.route("/health", methods=["GET"])
    def health():
        """Health check."""
        return jsonify({"status": "ok", "backend_url": backend_url}), 200

    return app


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    """Main entry point for running the proxy from command line."""
    import logging as _logging
    _logging.basicConfig(level=logging.INFO)
    app = create_proxy_app()
    logger.info("AsyncServiceProxy starting on port %d", PROXY_PORT)
    app.run(host="0.0.0.0", port=PROXY_PORT, threaded=True)


if __name__ == "__main__":
    main()
