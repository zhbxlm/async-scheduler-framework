"""Structured JSON logging configuration.

Replaces the default plain-text logs with JSON-formatted entries that
can be parsed by log aggregators (ELK, Loki, CloudWatch, etc.).

Usage:
    from src.common.logging_config import configure_logging
    configure_logging(level="INFO", json_output=True)

Fields in every log record:
    timestamp, level, logger, message, module, lineno
    + any extra keyword arguments passed to logger calls
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any


class _JsonFormatter(logging.Formatter):
    """Format log records as single-line JSON objects."""

    LEVEL_MAP = {
        logging.DEBUG:    "DEBUG",
        logging.INFO:     "INFO",
        logging.WARNING:  "WARNING",
        logging.ERROR:    "ERROR",
        logging.CRITICAL: "CRITICAL",
    }

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level":     self.LEVEL_MAP.get(record.levelno, "UNKNOWN"),
            "logger":    record.name,
            "message":   record.getMessage(),
            "module":    record.module,
            "lineno":    record.lineno,
        }

        # Attach exception info
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)

        # Attach extra fields (passed as logger.info("msg", extra={...}))
        skip = {
            "name", "msg", "args", "created", "filename", "funcName",
            "levelname", "levelno", "lineno", "module", "msecs", "message",
            "pathname", "process", "processName", "relativeCreated",
            "thread", "threadName", "exc_info", "exc_text", "stack_info",
        }
        for key, value in record.__dict__.items():
            if key not in skip:
                try:
                    json.dumps(value)   # ensure JSON-serialisable
                    entry[key] = value
                except (TypeError, ValueError):
                    entry[key] = str(value)

        return json.dumps(entry, ensure_ascii=False)


def configure_logging(
    level: str = "INFO",
    json_output: bool | None = None,
) -> None:
    """Configure root logger.

    Args:
        level: Log level string (DEBUG/INFO/WARNING/ERROR).
        json_output: Use JSON formatter. Defaults to True when
                     ENVIRONMENT != 'development' or when stdout is
                     not a TTY (i.e. running in a container).
    """
    if json_output is None:
        env = os.getenv("ENVIRONMENT", "development")
        json_output = env != "development" or not sys.stdout.isatty()

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Remove existing handlers to avoid duplicate output
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    if json_output:
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-8s %(name)-30s %(message)s",
                datefmt="%H:%M:%S",
            )
        )
    root.addHandler(handler)

    # Quiet noisy libraries
    for noisy in ("uvicorn.access", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logging.getLogger(__name__).info(
        "Logging configured: level=%s json=%s", level, json_output
    )
