"""Structured JSON logging for distributed ES ingestion.

Usage
-----
1. In CLI/app startup::

    from async_scheduler.observability.logging import configure_logging
    configure_logging(service="scheduler-api", node_id=os.environ.get("NODE_ID"))

2. In task execution context::

    from async_scheduler.observability.logging import task_context
    with task_context(task_id="xxx", attempt_id="yyy", capability="echo"):
        ...  # all log calls inside inherit these fields automatically

3. In worker/consumer code (no changes needed — context propagates via contextvars)

Environment variables
---------------------
LOG_FORMAT      json | text   (default: json)
LOG_LEVEL       DEBUG | INFO | WARNING | ERROR  (default: INFO)
LOG_ES_HOST     Elasticsearch host (optional, activates ES HTTP handler)
LOG_ES_INDEX    ES index name (default: scheduler-logs)
NODE_ID         Logical node identifier in the cluster
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import platform
import socket
import sys
import threading
import traceback
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Context variables — automatically propagated within asyncio tasks
# ---------------------------------------------------------------------------

_ctx_task_id: ContextVar[str | None] = ContextVar("task_id", default=None)
_ctx_attempt_id: ContextVar[str | None] = ContextVar("attempt_id", default=None)
_ctx_capability: ContextVar[str | None] = ContextVar("capability", default=None)
_ctx_worker_id: ContextVar[str | None] = ContextVar("worker_id", default=None)
_ctx_dag_id: ContextVar[str | None] = ContextVar("dag_id", default=None)
_ctx_tenant_id: ContextVar[str | None] = ContextVar("tenant_id", default=None)
_ctx_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)

# Static host info (resolved once at import time)
_HOSTNAME: str = socket.gethostname()
_IP: str = ""
try:
    _IP = socket.gethostbyname(_HOSTNAME)
except Exception:
    pass
_PID: int = os.getpid()
_PYTHON_VERSION: str = platform.python_version()


class task_context:
    """Context manager that injects task metadata into all log records.

    Can be used as a context manager or async context manager::

        with task_context(task_id="t1", capability="echo"):
            logger.info("processing")

        async with task_context(task_id="t1", attempt_id="a1"):
            await do_work()
    """

    def __init__(
        self,
        task_id: str | None = None,
        attempt_id: str | None = None,
        capability: str | None = None,
        worker_id: str | None = None,
        dag_id: str | None = None,
        tenant_id: str | None = None,
        request_id: str | None = None,
    ) -> None:
        self._tokens: list[Any] = []
        self._fields = {
            _ctx_task_id: task_id,
            _ctx_attempt_id: attempt_id,
            _ctx_capability: capability,
            _ctx_worker_id: worker_id,
            _ctx_dag_id: dag_id,
            _ctx_tenant_id: tenant_id,
            _ctx_request_id: request_id,
        }

    def __enter__(self) -> "task_context":
        for var, value in self._fields.items():
            if value is not None:
                self._tokens.append((var, var.set(value)))
        return self

    def __exit__(self, *_: Any) -> None:
        for var, token in self._tokens:
            var.reset(token)

    async def __aenter__(self) -> "task_context":
        return self.__enter__()

    async def __aexit__(self, *args: Any) -> None:
        return self.__exit__(*args)


# ---------------------------------------------------------------------------
# JSON log formatter
# ---------------------------------------------------------------------------

class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects for ES ingestion.

    Each record includes:
    - @timestamp (ISO8601 UTC)
    - level, logger, message
    - host.hostname, host.ip, host.pid
    - service.name, service.node_id, service.version
    - task_id, attempt_id, capability, worker_id, dag_id, tenant_id, request_id
      (from contextvars — only present when non-None)
    - error.type, error.message, error.stack_trace (on exception)
    - extra fields passed via logger.info("...", extra={...})
    """

    _SKIP_ATTRS = frozenset({
        "args", "created", "exc_info", "exc_text", "filename", "funcName",
        "levelname", "levelno", "lineno", "message", "module", "msecs",
        "msg", "name", "pathname", "process", "processName", "relativeCreated",
        "stack_info", "thread", "threadName", "taskName",
    })

    def __init__(
        self,
        service: str = "async-scheduler",
        node_id: str | None = None,
        version: str | None = None,
    ) -> None:
        super().__init__()
        self._service = service
        self._node_id = node_id or os.environ.get("NODE_ID", _HOSTNAME)
        self._version = version or os.environ.get("SERVICE_VERSION", "")

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()

        doc: dict[str, Any] = {
            "@timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.message,
            # Location
            "log.origin.file": record.filename,
            "log.origin.function": record.funcName,
            "log.origin.line": record.lineno,
            # Host
            "host.hostname": _HOSTNAME,
            "host.ip": _IP,
            "host.pid": _PID,
            # Service
            "service.name": self._service,
            "service.node_id": self._node_id,
        }

        if self._version:
            doc["service.version"] = self._version

        # Context variables (task execution context)
        _add_if_set(doc, "task_id", _ctx_task_id.get())
        _add_if_set(doc, "attempt_id", _ctx_attempt_id.get())
        _add_if_set(doc, "capability", _ctx_capability.get())
        _add_if_set(doc, "worker_id", _ctx_worker_id.get())
        _add_if_set(doc, "dag_id", _ctx_dag_id.get())
        _add_if_set(doc, "tenant_id", _ctx_tenant_id.get())
        _add_if_set(doc, "request_id", _ctx_request_id.get())

        # Thread info (useful for multi-threaded workers)
        if threading.current_thread().name != "MainThread":
            doc["thread.name"] = threading.current_thread().name

        # Exception info
        if record.exc_info and record.exc_info[0] is not None:
            exc_type, exc_value, exc_tb = record.exc_info
            doc["error.type"] = exc_type.__qualname__
            doc["error.message"] = str(exc_value)
            doc["error.stack_trace"] = "".join(
                traceback.format_exception(exc_type, exc_value, exc_tb)
            ).rstrip()

        # Extra fields (caller-supplied via extra={...})
        for key, value in record.__dict__.items():
            if key not in self._SKIP_ATTRS and not key.startswith("_"):
                doc[f"extra.{key}"] = value

        return json.dumps(doc, default=str, ensure_ascii=False)


def _add_if_set(doc: dict, key: str, value: str | None) -> None:
    if value is not None:
        doc[key] = value


# ---------------------------------------------------------------------------
# Optional ES HTTP handler (non-blocking, buffered)
# ---------------------------------------------------------------------------

class ElasticsearchHandler(logging.Handler):
    """Best-effort async HTTP handler that ships log batches to ES.

    Uses a background thread + in-memory queue to avoid blocking the
    event loop. Drops oldest records when the queue is full.
    """

    def __init__(
        self,
        host: str,
        index: str = "scheduler-logs",
        batch_size: int = 50,
        flush_interval: float = 2.0,
        queue_maxsize: int = 10_000,
        timeout: float = 5.0,
    ) -> None:
        super().__init__()
        self._host = host.rstrip("/")
        self._index = index
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._timeout = timeout
        self._queue: list[str] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._flush_loop, name="es-log-flusher", daemon=True
        )
        self._thread.start()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
            with self._lock:
                self._queue.append(line)
                # Drop oldest when over capacity
                if len(self._queue) > 10_000:
                    self._queue.pop(0)
        except Exception:
            self.handleError(record)

    def _flush_loop(self) -> None:
        while not self._stop_event.wait(self._flush_interval):
            self._flush()

    def _flush(self) -> None:
        with self._lock:
            if not self._queue:
                return
            batch, self._queue = self._queue[: self._batch_size], self._queue[self._batch_size :]

        bulk_body = ""
        for line in batch:
            bulk_body += json.dumps({"index": {"_index": self._index}}) + "\n"
            bulk_body += line + "\n"

        try:
            import urllib.request
            req = urllib.request.Request(
                f"{self._host}/_bulk",
                data=bulk_body.encode(),
                headers={"Content-Type": "application/x-ndjson"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:  # noqa: S310
                if resp.status >= 400:
                    sys.stderr.write(f"[es-log] bulk failed: {resp.status}\n")
        except Exception as exc:
            sys.stderr.write(f"[es-log] flush error: {exc}\n")

    def close(self) -> None:
        self._stop_event.set()
        self._flush()
        super().close()


# ---------------------------------------------------------------------------
# Top-level configure function
# ---------------------------------------------------------------------------

def configure_logging(
    service: str = "async-scheduler",
    node_id: str | None = None,
    version: str | None = None,
    level: str | None = None,
    fmt: str | None = None,
) -> None:
    """Configure root logger with structured JSON output.

    Call once at application startup (CLI entry point or lifespan).

    Parameters
    ----------
    service:  Logical service name (e.g. "scheduler-api", "scheduler-worker")
    node_id:  Cluster node identifier; defaults to NODE_ID env or hostname
    version:  Deployed service version; defaults to SERVICE_VERSION env
    level:    Log level string; defaults to LOG_LEVEL env or INFO
    fmt:      "json" or "text"; defaults to LOG_FORMAT env or "json"
    """
    effective_level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    effective_fmt = fmt or os.environ.get("LOG_FORMAT", "json")

    root = logging.getLogger()
    root.setLevel(effective_level)

    # Remove any existing handlers (avoid duplicate logs from uvicorn/click)
    root.handlers.clear()

    if effective_fmt == "json":
        formatter: logging.Formatter = JsonFormatter(
            service=service, node_id=node_id, version=version
        )
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )

    # Stdout handler (always)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    root.addHandler(stdout_handler)

    # Optional ES handler
    es_host = os.environ.get("LOG_ES_HOST")
    if es_host:
        es_index = os.environ.get("LOG_ES_INDEX", "scheduler-logs")
        es_handler = ElasticsearchHandler(
            host=es_host,
            index=es_index,
        )
        es_handler.setFormatter(formatter)
        root.addHandler(es_handler)
        logging.getLogger(__name__).info(
            "Elasticsearch log handler enabled",
            extra={"es_host": es_host, "es_index": es_index},
        )
