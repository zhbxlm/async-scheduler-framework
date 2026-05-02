"""Tests for structured logging."""
import json
import logging
import socket
import os
import sys

import pytest


def test_json_formatter_basic():
    """JSON output has required fields."""
    from async_scheduler.observability import configure_logging
    from async_scheduler.observability import JsonFormatter

    fmt = JsonFormatter(service="test-svc", node_id="node-01")
    record = logging.LogRecord(
        name="test.logger", level=logging.INFO,
        pathname=__file__, lineno=1,
        msg="hello world", args=(), exc_info=None,
    )
    output = fmt.format(record)
    doc = json.loads(output)

    assert doc["message"] == "hello world"
    assert doc["level"] == "INFO"
    assert doc["service.name"] == "test-svc"
    assert doc["service.node_id"] == "node-01"
    assert doc["host.hostname"] == socket.gethostname()
    assert doc["host.pid"] == os.getpid()
    assert "@timestamp" in doc
    assert "log.origin.file" in doc


def test_json_formatter_exception():
    """Exception info is captured as structured error fields."""
    from async_scheduler.observability import JsonFormatter

    fmt = JsonFormatter()
    try:
        raise ValueError("boom")
    except ValueError:
        exc_info = sys.exc_info()

    record = logging.LogRecord(
        name="test", level=logging.ERROR,
        pathname=__file__, lineno=1,
        msg="task failed", args=(), exc_info=exc_info,
    )
    doc = json.loads(fmt.format(record))

    assert doc["error.type"] == "ValueError"
    assert "boom" in doc["error.message"]
    assert "Traceback" in doc["error.stack_trace"]


def test_task_context_propagation():
    """task_context injects task_id / capability into log records."""
    from async_scheduler.observability import JsonFormatter, task_context

    fmt = JsonFormatter()

    with task_context(task_id="task-123", capability="echo", worker_id="w-01"):
        record = logging.LogRecord(
            name="test", level=logging.INFO,
            pathname=__file__, lineno=1,
            msg="processing", args=(), exc_info=None,
        )
        doc = json.loads(fmt.format(record))
        assert doc["task_id"] == "task-123"
        assert doc["capability"] == "echo"
        assert doc["worker_id"] == "w-01"

    # After context exits, fields are gone
    record2 = logging.LogRecord(
        name="test", level=logging.INFO,
        pathname=__file__, lineno=1,
        msg="after context", args=(), exc_info=None,
    )
    doc2 = json.loads(fmt.format(record2))
    assert "task_id" not in doc2
    assert "capability" not in doc2


def test_task_context_nesting():
    """Nested task_context correctly restores outer values."""
    from async_scheduler.observability import JsonFormatter, task_context

    fmt = JsonFormatter()

    def get_task_id() -> str | None:
        r = logging.LogRecord("t", logging.INFO, __file__, 1, "x", (), None)
        return json.loads(fmt.format(r)).get("task_id")

    with task_context(task_id="outer"):
        assert get_task_id() == "outer"
        with task_context(task_id="inner"):
            assert get_task_id() == "inner"
        assert get_task_id() == "outer"
    assert get_task_id() is None


def test_configure_logging_json(capsys):
    """configure_logging produces valid JSON to stdout."""
    from async_scheduler.observability import configure_logging

    configure_logging(service="test", fmt="json", level="DEBUG")
    logging.getLogger("test.structured").info("structured log test")
    captured = capsys.readouterr()
    # Find the JSON line
    for line in captured.out.strip().splitlines():
        if "structured log test" in line:
            doc = json.loads(line)
            assert doc["message"] == "structured log test"
            assert doc["level"] == "INFO"
            return
    pytest.fail("Expected structured log line not found in stdout")


def test_configure_logging_text(capsys):
    """configure_logging in text mode produces plain text."""
    from async_scheduler.observability import configure_logging

    configure_logging(service="test", fmt="text", level="DEBUG")
    logging.getLogger("test.text").info("plain text test")
    captured = capsys.readouterr()
    assert "plain text test" in captured.out
    # Should NOT be JSON
    for line in captured.out.strip().splitlines():
        if "plain text test" in line:
            with pytest.raises(json.JSONDecodeError):
                json.loads(line)
            return
