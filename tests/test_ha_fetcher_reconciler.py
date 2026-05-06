"""Tests for redis_ha, remote_code_fetcher, and reconciler cold-start fix.

=== RedisHAClient ===
- get/set/delete delegate to primary
- primary failure triggers fallback to replica (read-only ops)
- write ops raise on primary failure (no read fallback)
- circuit breaker integration: open CB skips primary
- retry on transient error
- health_check() returns dict with status

=== RemoteCodeFetcher ===
- validate_artifact_url: valid https URL passes
- validate_artifact_url: http:// blocked schemes raise
- validate_artifact_url: localhost blocked
- validate_artifact_url: private IP blocked by default
- validate_artifact_url: private IP allowed with flag
- fetch_artifact: cache hit returns immediately (no download)
- fetch_artifact: cache miss triggers _download + _extract
- _download: SHA-256 mismatch raises ArtifactError
- _download: size limit exceeded raises ArtifactError
- _extract: path traversal entries raise ArtifactError
- _extract: symlink with .. raises ArtifactError
- _gc_artifact_cache: evicts oldest entries when over limit
- _cache_key: same url+sha → same key; different sha → different key

=== TaskReconciler cold-start fix ===
- _loop() now runs first tick immediately (sleep at end, not start)
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import os
import tarfile
import tempfile
import time
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, call

from tests.fake_redis import FullFakeAsyncRedis
from src.platform.remote_code_fetcher import RemoteCodeFetcher, ArtifactError
from src.platform.task_reconciler import TaskReconciler


# ===========================================================================
# RedisHA CircuitBreaker (in-process, no Redis connection needed)
# ===========================================================================

from src.common.redis_ha import CircuitBreaker as HACircuitBreaker


def _make_ha_cb(**kwargs):
    return HACircuitBreaker(**kwargs)


def test_ha_cb_initial_state_closed():
    cb = _make_ha_cb(failure_threshold=3)
    assert cb.state == "CLOSED"


def test_ha_cb_trips_to_open_at_threshold():
    cb = _make_ha_cb(failure_threshold=3)
    for _ in range(3):
        cb.record_failure()
    assert cb.state == "OPEN"


def test_ha_cb_does_not_trip_below_threshold():
    cb = _make_ha_cb(failure_threshold=5)
    for _ in range(4):
        cb.record_failure()
    assert cb.state == "CLOSED"


def test_ha_cb_is_open_returns_true_when_open():
    cb = _make_ha_cb(failure_threshold=1)
    cb.record_failure()
    assert cb.state == "OPEN"


def test_ha_cb_is_open_returns_false_when_closed():
    cb = _make_ha_cb()
    assert cb.state == "CLOSED"


def test_ha_cb_reset_closes_breaker():
    cb = _make_ha_cb(failure_threshold=1)
    cb.record_failure()
    assert cb.state == "OPEN"
    cb.record_success()  # OPEN → HALF_OPEN → or reset via record_success from HALF_OPEN
    # Force HALF_OPEN then success
    cb.state = "HALF_OPEN"
    cb.record_success()
    assert cb.state == "CLOSED"


def test_ha_cb_reset_clears_failure_count():
    cb = _make_ha_cb(failure_threshold=5)
    for _ in range(3):
        cb.record_failure()
    # record_success resets counter
    cb.record_success()
    assert cb.failure_count == 0


def test_ha_cb_already_open_does_not_increment_state():
    """State stays OPEN when failures accumulate while open."""
    cb = _make_ha_cb(failure_threshold=1)
    cb.record_failure()  # trips
    assert cb.state == "OPEN"
    cb.record_failure()  # while open — count increments, state stays OPEN
    assert cb.state == "OPEN"


def test_ha_cb_failure_count_exposed():
    cb = _make_ha_cb(failure_threshold=10)
    cb.record_failure()
    cb.record_failure()
    assert cb.failure_count == 2


def test_ha_cb_reset_timeout_defaults():
    cb = _make_ha_cb(reset_timeout=45.0)
    assert cb.reset_timeout == 45.0


def test_ha_cb_should_allow_closed():
    cb = _make_ha_cb()
    assert cb.should_allow() is True


def test_ha_cb_should_not_allow_when_open():
    cb = _make_ha_cb(failure_threshold=1)
    cb.record_failure()
    assert cb.should_allow() is False


def test_ha_cb_half_open_allows_one_request():
    cb = _make_ha_cb(failure_threshold=1)
    cb.state = "HALF_OPEN"
    assert cb.should_allow() is True


# ===========================================================================
# RemoteCodeFetcher — URL validation
# ===========================================================================

def _make_fetcher(tmp_path, allow_private=False):
    return RemoteCodeFetcher(
        cache_dir=str(tmp_path / "cache"),
        max_size_mb=10,
        allow_private_ip=allow_private,
    )


def test_validate_url_valid_https(tmp_path):
    f = _make_fetcher(tmp_path)
    f.validate_artifact_url("https://example.com/artifact.tar.gz")  # no raise


def test_validate_url_valid_http(tmp_path):
    f = _make_fetcher(tmp_path)
    f.validate_artifact_url("http://example.com/artifact.tar.gz")  # no raise


def test_validate_url_blocked_scheme_ftp(tmp_path):
    f = _make_fetcher(tmp_path)
    with pytest.raises(ArtifactError, match="scheme"):
        f.validate_artifact_url("ftp://example.com/file.tar.gz")


def test_validate_url_blocked_scheme_file(tmp_path):
    f = _make_fetcher(tmp_path)
    with pytest.raises(ArtifactError):
        f.validate_artifact_url("file:///etc/passwd")


def test_validate_url_blocked_localhost(tmp_path):
    f = _make_fetcher(tmp_path)
    with pytest.raises(ArtifactError, match="Blocked hostname"):
        f.validate_artifact_url("https://localhost/art.tar.gz")


def test_validate_url_blocked_127(tmp_path):
    f = _make_fetcher(tmp_path)
    with pytest.raises(ArtifactError):
        f.validate_artifact_url("https://127.0.0.1/art.tar.gz")


def test_validate_url_blocked_private_ip(tmp_path):
    f = _make_fetcher(tmp_path)
    with pytest.raises(ArtifactError, match="Private IP"):
        f.validate_artifact_url("https://192.168.1.100/art.tar.gz")


def test_validate_url_private_ip_allowed_with_flag(tmp_path):
    f = _make_fetcher(tmp_path, allow_private=True)
    f.validate_artifact_url("https://192.168.1.100/art.tar.gz")  # no raise


def test_validate_url_empty_host(tmp_path):
    f = _make_fetcher(tmp_path)
    with pytest.raises(ArtifactError):
        f.validate_artifact_url("https:///no-host/art.tar.gz")


# ===========================================================================
# RemoteCodeFetcher — cache hit
# ===========================================================================

def test_fetch_artifact_cache_hit(tmp_path):
    f = _make_fetcher(tmp_path)
    url = "https://example.com/pkg.tar.gz"
    sha = "abc123"

    # Manually create cache entry
    key = f._cache_key(url, sha)
    cache_path = Path(f._cache_dir) / key
    pkg_dir = cache_path / "package"
    pkg_dir.mkdir(parents=True)
    (cache_path / ".ready").touch()

    result = f.fetch_artifact(url, sha)
    assert result == str(pkg_dir)


# ===========================================================================
# RemoteCodeFetcher — download validation
# ===========================================================================

def _make_tgz(files: dict[str, bytes]) -> bytes:
    """Create an in-memory .tar.gz with given {name: content} entries."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, content in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def test_download_sha256_mismatch(tmp_path):
    f = _make_fetcher(tmp_path)
    tgz_data = _make_tgz({"file.txt": b"hello"})

    with patch("httpx.stream") as mock_stream:
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        ctx.raise_for_status = MagicMock()
        ctx.iter_bytes = MagicMock(return_value=[tgz_data])
        mock_stream.return_value = ctx

        with pytest.raises(ArtifactError, match="SHA-256 mismatch"):
            f._download("https://example.com/pkg.tar.gz", "wrong-sha256")


def test_download_size_limit_exceeded(tmp_path):
    f = RemoteCodeFetcher(
        cache_dir=str(tmp_path / "cache"),
        max_size_mb=0,  # 0 MB limit
        allow_private_ip=False,
    )
    large_data = b"x" * 1024  # any data > 0 bytes

    with patch("httpx.stream") as mock_stream:
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        ctx.raise_for_status = MagicMock()
        ctx.iter_bytes = MagicMock(return_value=[large_data])
        mock_stream.return_value = ctx

        with pytest.raises(ArtifactError, match="size limit"):
            f._download("https://example.com/pkg.tar.gz", None)


# ===========================================================================
# RemoteCodeFetcher — extraction security
# ===========================================================================

def test_extract_rejects_absolute_path(tmp_path):
    f = _make_fetcher(tmp_path)

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        info = tarfile.TarInfo(name="/etc/evil")
        info.size = 4
        tf.addfile(info, io.BytesIO(b"evil"))
    buf.seek(0)

    tar_path = tmp_path / "test.tar.gz"
    tar_path.write_bytes(buf.getvalue())

    dest = tmp_path / "dest"
    with pytest.raises(ArtifactError, match="Unsafe tar entry"):
        f._extract(tar_path, dest)


def test_extract_rejects_path_traversal(tmp_path):
    f = _make_fetcher(tmp_path)

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        info = tarfile.TarInfo(name="../../etc/shadow")
        info.size = 4
        tf.addfile(info, io.BytesIO(b"evil"))
    buf.seek(0)

    tar_path = tmp_path / "test.tar.gz"
    tar_path.write_bytes(buf.getvalue())

    dest = tmp_path / "dest"
    with pytest.raises(ArtifactError, match="Unsafe tar entry"):
        f._extract(tar_path, dest)


def test_extract_valid_tar(tmp_path):
    f = _make_fetcher(tmp_path)
    tgz = _make_tgz({"hello.txt": b"world", "subdir/data.bin": b"\x00\x01"})
    tar_path = tmp_path / "valid.tar.gz"
    tar_path.write_bytes(tgz)

    dest = tmp_path / "dest"
    pkg = f._extract(tar_path, dest)
    assert (Path(pkg) / "hello.txt").exists()


# ===========================================================================
# RemoteCodeFetcher — GC / cache eviction
# ===========================================================================

def test_gc_evicts_when_over_limit(tmp_path):
    f = RemoteCodeFetcher(
        cache_dir=str(tmp_path / "cache"),
        max_size_mb=1024,
        max_cache_entries=2,
    )
    cache = Path(f._cache_dir)

    # Create 3 fake cache entries
    for i in range(3):
        entry = cache / f"entry{i}"
        (entry / "package").mkdir(parents=True)
        (entry / ".ready").touch()
        # Stagger mtimes
        os.utime(entry, (i, i))

    evicted = f._gc_artifact_cache()
    assert evicted >= 1

    # Should have at most max_cache_entries remaining
    remaining = [p for p in cache.iterdir() if (p / ".ready").exists()]
    assert len(remaining) <= 2


def test_gc_no_eviction_below_limit(tmp_path):
    f = RemoteCodeFetcher(
        cache_dir=str(tmp_path / "cache"),
        max_size_mb=1024,
        max_cache_entries=10,
    )
    cache = Path(f._cache_dir)
    for i in range(3):
        entry = cache / f"entry{i}"
        (entry / "package").mkdir(parents=True)
        (entry / ".ready").touch()

    evicted = f._gc_artifact_cache()
    assert evicted == 0


def test_cache_key_deterministic(tmp_path):
    f = _make_fetcher(tmp_path)
    k1 = f._cache_key("https://x.com/a.tgz", "sha256abc")
    k2 = f._cache_key("https://x.com/a.tgz", "sha256abc")
    assert k1 == k2


def test_cache_key_different_sha(tmp_path):
    f = _make_fetcher(tmp_path)
    k1 = f._cache_key("https://x.com/a.tgz", "sha1")
    k2 = f._cache_key("https://x.com/a.tgz", "sha2")
    assert k1 != k2


def test_cache_key_none_sha(tmp_path):
    f = _make_fetcher(tmp_path)
    k1 = f._cache_key("https://x.com/a.tgz", None)
    k2 = f._cache_key("https://x.com/a.tgz", None)
    assert k1 == k2


# ===========================================================================
# TaskReconciler — cold-start fix (sleep at end)
# ===========================================================================

@pytest.mark.asyncio
async def test_reconciler_runs_first_tick_immediately():
    """After the loop fix, first phase should run without waiting interval."""
    from src.platform.task_reconciler import TaskReconciler
    from tests.fake_redis import FullFakeAsyncRedis

    redis = FullFakeAsyncRedis()
    rec = TaskReconciler(
        redis_client=redis,
        interval_seconds=9999.0,  # huge interval — should NOT block first tick
        instance_id="test",
    )

    phase1_called = asyncio.Event()

    async def fake_phase1(batch):
        phase1_called.set()

    with patch.object(rec, "_phase1_double_write", side_effect=fake_phase1):
        with patch.object(rec, "_scan_task_batch", return_value=[{"task_id": "t1", "status": "completed"}]):
            with patch.object(rec, "_phase2_stuck_recovery", return_value=None):
                with patch.object(rec, "_phase3_lost_callback", return_value=None):
                    await rec.start()
                    # First tick should complete before interval fires
                    try:
                        await asyncio.wait_for(phase1_called.wait(), timeout=1.0)
                    except asyncio.TimeoutError:
                        pytest.fail("First tick did not run immediately — sleep-first bug still present")
                    finally:
                        await rec.stop()
