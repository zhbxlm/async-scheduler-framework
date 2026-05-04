"""AsyncCommandProxy — aligned with docs/deepwiki-reference/异步代理.md

Sidecar proxy: executes local shell/Python commands via subprocess,
stores results in Redis, Worker retrieves via BLPOP.

Endpoints:
  POST /submit     → {job_id, result_key}
  POST /cancel/<id>→ {cancelled: true}
  GET  /result/<id>→ {status, data, ...}
  GET  /health     → {status: ok}

Config (env vars, 3-tier priority):
  ASYNC_COMMAND_PROXY_PORT  default 5002
  REDIS_URL                 default redis://localhost:6379/0
  RESULT_TTL                default 3600 (seconds)
  MAX_WORKERS               default 8
  DEFAULT_TIMEOUT           default 300 (seconds)
"""
from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROXY_PORT      = int(os.getenv("ASYNC_COMMAND_PROXY_PORT", "5002"))
REDIS_URL       = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RESULT_TTL      = int(os.getenv("RESULT_TTL", "3600"))
MAX_WORKERS     = int(os.getenv("MAX_WORKERS", "8"))
DEFAULT_TIMEOUT = int(os.getenv("DEFAULT_TIMEOUT", "300"))

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_executor    = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="cmd_proxy")
_cancelled: set[str] = set()
_processes: dict[str, subprocess.Popen] = {}
_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Redis helper
# ---------------------------------------------------------------------------

def _get_redis():
    import redis as _redis
    return _redis.from_url(REDIS_URL, decode_responses=True)


def _result_key(job_id: str) -> str:
    return f"cmd_result:{job_id}"


def _write_result(job_id: str, payload: dict) -> None:
    r = _get_redis()
    key = _result_key(job_id)
    r.lpush(key, json.dumps(payload))
    r.expire(key, RESULT_TTL)


# ---------------------------------------------------------------------------
# Command execution
# ---------------------------------------------------------------------------

def _build_cmd(command: str, params: dict | None) -> list[str]:
    """Build subprocess command list from command string + params dict."""
    parts = shlex.split(command)
    if params:
        for k, v in params.items():
            parts.append(f"--{k}")
            if v is not None and v != "":
                parts.append(str(v))
    return parts


def _execute_and_store(
    job_id: str,
    command: str,
    params: dict | None,
    env_extra: dict | None,
    cwd: str | None,
    timeout: int,
) -> None:
    """Background thread: run command, store result in Redis."""
    cmd = _build_cmd(command, params)
    env = {**os.environ, **(env_extra or {})}
    t0 = time.time()
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=cwd or None,
        )
        with _lock:
            _processes[job_id] = proc

        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
            with _lock:
                _processes.pop(job_id, None)
            with _lock:
                was_cancelled = job_id in _cancelled
            if not was_cancelled:
                _write_result(job_id, {
                    "status": "error",
                    "error": "timeout",
                    "elapsed": round(time.time() - t0, 3),
                })
            return

        with _lock:
            _processes.pop(job_id, None)
            was_cancelled = job_id in _cancelled

        if was_cancelled:
            return  # cancel handler already wrote result

        elapsed = round(time.time() - t0, 3)
        if proc.returncode == 0:
            _write_result(job_id, {
                "status": "success",
                "returncode": 0,
                "stdout": stdout.decode(errors="replace") if stdout else "",
                "elapsed": elapsed,
            })
        else:
            _write_result(job_id, {
                "status": "error",
                "returncode": proc.returncode,
                "stdout": stdout.decode(errors="replace") if stdout else "",
                "stderr": stderr.decode(errors="replace") if stderr else "",
                "elapsed": elapsed,
            })
    except Exception as exc:
        logger.error("AsyncCommandProxy: job_id=%s execute error: %s", job_id, exc)
        _write_result(job_id, {"status": "error", "error": str(exc)})
    finally:
        with _lock:
            _processes.pop(job_id, None)


# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------

try:
    from flask import Flask, jsonify, request as flask_request

    app = Flask(__name__)

    @app.post("/submit")
    def submit():
        body = flask_request.get_json(force=True) or {}
        command = body.get("command")
        if not command:
            return jsonify({"error": "command is required"}), 400

        params      = body.get("params") or {}
        env_extra   = body.get("env") or {}
        cwd         = body.get("cwd")
        timeout     = int(body.get("timeout") or DEFAULT_TIMEOUT)
        job_id      = str(uuid.uuid4())
        result_key  = _result_key(job_id)

        _executor.submit(_execute_and_store, job_id, command, params, env_extra, cwd, timeout)
        return jsonify({"job_id": job_id, "result_key": result_key})

    @app.post("/cancel/<job_id>")
    def cancel(job_id: str):
        with _lock:
            _cancelled.add(job_id)
            proc = _processes.get(job_id)

        # Write cancellation result to Redis (unblocks BLPOP)
        _write_result(job_id, {"status": "cancelled", "job_id": job_id})

        if proc:
            try:
                proc.terminate()
            except Exception:
                pass
        return jsonify({"cancelled": True, "job_id": job_id})

    @app.get("/result/<job_id>")
    def result(job_id: str):
        r = _get_redis()
        raw = r.lindex(_result_key(job_id), 0)
        if raw is None:
            return jsonify({"status": "pending", "job_id": job_id})
        return jsonify(json.loads(raw))

    @app.get("/health")
    def health():
        try:
            _get_redis().ping()
            redis_ok = True
        except Exception:
            redis_ok = False
        return jsonify({"status": "ok" if redis_ok else "degraded", "redis_ok": redis_ok})

except ImportError:
    app = None  # type: ignore[assignment]
    logger.warning("AsyncCommandProxy: Flask not installed, HTTP server disabled")


# ---------------------------------------------------------------------------
# Programmatic API (used by AsyncProxyWorker in non-HTTP mode)
# ---------------------------------------------------------------------------

class AsyncCommandProxy:
    """Programmatic wrapper — submit commands and retrieve results."""

    def __init__(self, endpoint: str = ""):
        self._endpoint = endpoint or f"http://localhost:{PROXY_PORT}"
        self._redis = _get_redis()

    def submit(
        self,
        command: str,
        params: dict | None = None,
        env: dict | None = None,
        cwd: str | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> dict:
        """Submit command for async execution; return {job_id, result_key}."""
        job_id = str(uuid.uuid4())
        result_key = _result_key(job_id)
        _executor.submit(_execute_and_store, job_id, command, params, env, cwd, timeout)
        return {"job_id": job_id, "result_key": result_key}

    def blpop_result(self, result_key: str, timeout: int = DEFAULT_TIMEOUT) -> dict | None:
        """Block until result is available; return parsed result dict."""
        resp = self._redis.blpop(result_key, timeout=timeout)
        if resp is None:
            return None
        _, raw = resp
        return json.loads(raw)

    def cancel(self, job_id: str) -> bool:
        with _lock:
            _cancelled.add(job_id)
            proc = _processes.get(job_id)
        _write_result(job_id, {"status": "cancelled", "job_id": job_id})
        if proc:
            try:
                proc.terminate()
            except Exception:
                pass
        return True

    async def execute(self, cmd: str, args: list[str]) -> dict:
        """Async convenience wrapper for CLI-style invocation."""
        import asyncio
        loop = asyncio.get_event_loop()
        full_cmd = cmd + (" " + " ".join(args) if args else "")
        submit_resp = self.submit(full_cmd)
        result = await loop.run_in_executor(
            None, self.blpop_result, submit_resp["result_key"]
        )
        return result or {"status": "timeout", "job_id": submit_resp["job_id"]}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    if app is None:
        raise RuntimeError("Flask is required to run AsyncCommandProxy server")
    logging.basicConfig(level=logging.INFO)
    logger.info("AsyncCommandProxy starting on port %d", PROXY_PORT)
    app.run(host="0.0.0.0", port=PROXY_PORT, threaded=True)


if __name__ == "__main__":
    main()
