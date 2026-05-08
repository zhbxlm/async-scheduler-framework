"""Tests for AsyncProxyWorker — updated for substantive implementation."""
from unittest.mock import MagicMock

import pytest

from src.workload.async_proxy_worker import AsyncProxyWorker, ProxySubmitError, ProxyTimeoutError


def test_async_proxy_worker_init():
    """AsyncProxyWorker initialises with config dict."""
    worker = AsyncProxyWorker(
        capability="cap_test",
        config={"proxy_url": "http://mock", "redis_url": "redis://localhost:6379/0"},
    )
    assert worker._proxy_url == "http://mock"
    assert worker.capability == "cap_test"


def test_async_proxy_worker_transform_passthrough():
    """Default transform_input/output are identity."""
    worker = AsyncProxyWorker(config={})
    data = {"key": "value"}
    assert worker.transform_input(data) == data
    assert worker.transform_output(data) == data


def test_async_proxy_worker_missing_backend_path():
    """call() raises ValueError if backend_path not set."""
    worker = AsyncProxyWorker(config={})
    worker.backend_path = ""
    with pytest.raises(ValueError, match="backend_path"):
        worker.call({"x": 1})


def test_async_proxy_worker_submit_error(monkeypatch):
    """call() raises ProxySubmitError when /submit request fails."""
    import requests as _requests
    worker = AsyncProxyWorker(config={"proxy_url": "http://mock"})
    worker.backend_path = "/generate"

    def _fail_post(*a, **kw):
        raise _requests.exceptions.ConnectionError("connection refused")

    monkeypatch.setattr("requests.post", _fail_post)
    with pytest.raises(ProxySubmitError):
        worker.call({"data": "x"})


def test_async_proxy_worker_timeout(monkeypatch):
    """call() raises ProxyTimeoutError when BLPOP returns None."""

    worker = AsyncProxyWorker(config={"proxy_url": "http://mock"})
    worker.backend_path = "/generate"
    worker.timeout = 1

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"job_id": "j1", "result_key": "cmd_result:j1"}
    monkeypatch.setattr("requests.post", lambda *a, **kw: mock_resp)

    mock_redis = MagicMock()
    mock_redis.blpop.return_value = None
    monkeypatch.setattr(worker, "_get_redis", lambda: mock_redis)

    with pytest.raises(ProxyTimeoutError):
        worker.call({"data": "x"})
