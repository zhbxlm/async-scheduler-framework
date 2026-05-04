"""Tests for RayDataClient, RemoteCodeFetcher and NodeRegistry."""
from __future__ import annotations

import json
import os
import tarfile
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models.cluster import ClusterInfo
from src.models.node import NodeInfo, NodeResources, NodeState
from src.models.task import TaskInfo, TaskStatus, TaskDispatchMode, TaskPriority
from src.platform.raydata_client import RayDataClient, _safe_json
from src.platform.remote_code_fetcher import ArtifactError, RemoteCodeFetcher


# ===========================================================================
# RayDataClient tests
# ===========================================================================

class TestRayDataClient:
    def _make_task(self, **kwargs) -> TaskInfo:
        defaults = dict(
            task_id="t1", task_type="inference", status=TaskStatus.PENDING,
            priority=TaskPriority.NORMAL,
            dispatch_mode=TaskDispatchMode.RAYDATA_NATIVE,
        )
        defaults.update(kwargs)
        return TaskInfo(**defaults)

    def _make_cluster(self, **kwargs) -> ClusterInfo:
        defaults = dict(cluster_id="c1", ray_head_address="http://10.0.0.1:8265")
        defaults.update(kwargs)
        return ClusterInfo(**defaults)

    def test_raises_without_ray_head_address(self):
        client = RayDataClient()
        cluster = self._make_cluster(ray_head_address="")
        task = self._make_task()
        with pytest.raises(RuntimeError, match="no ray_head_address"):
            client.submit_task(cluster, task)

    def test_standard_payload_built_correctly(self):
        task = self._make_task(
            input_data={"prompt": "hello"},
            callback_url="http://cb.example.com/done",
        )
        payload = RayDataClient._build_payload(task)
        assert payload["task_id"] == "t1"
        assert payload["task_type"] == "inference"
        assert payload["input_data"] == {"prompt": "hello"}
        assert payload["callback_url"] == "http://cb.example.com/done"

    def test_custom_raydata_request_used_verbatim(self):
        task = self._make_task(input_data={"raydata_request": {"custom": "payload"}})
        payload = RayDataClient._build_payload(task)
        assert payload == {"custom": "payload"}

    def test_bearer_token_header(self):
        client = RayDataClient(api_token="secret123")
        headers = client._build_headers()
        assert headers["Authorization"] == "Bearer secret123"

    def test_no_token_no_auth_header(self):
        client = RayDataClient()
        headers = client._build_headers()
        assert "Authorization" not in headers

    def test_submit_task_success(self):
        client = RayDataClient()
        cluster = self._make_cluster()
        task = self._make_task()

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"submission_id": "sub-abc", "status": "ok"}

        with patch("httpx.Client") as MockClient:
            MockClient.return_value.__enter__.return_value.post.return_value = mock_response
            result = client.submit_task(cluster, task)

        assert result["submission_id"] == "sub-abc"
        assert result["cluster_id"] == "c1"
        assert result["submit_url"] == "http://10.0.0.1:8265/api/jobs/"

    def test_submit_task_fallback_to_job_id(self):
        client = RayDataClient()
        cluster = self._make_cluster()
        task = self._make_task()

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"job_id": "job-xyz"}

        with patch("httpx.Client") as MockClient:
            MockClient.return_value.__enter__.return_value.post.return_value = mock_response
            result = client.submit_task(cluster, task)

        assert result["submission_id"] == "job-xyz"

    def test_submit_task_fallback_to_task_id(self):
        client = RayDataClient()
        cluster = self._make_cluster()
        task = self._make_task()

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"other_field": "value"}

        with patch("httpx.Client") as MockClient:
            MockClient.return_value.__enter__.return_value.post.return_value = mock_response
            result = client.submit_task(cluster, task)

        assert result["submission_id"] == "t1"  # fallback to task_id


# ===========================================================================
# RemoteCodeFetcher tests
# ===========================================================================

class TestRemoteCodeFetcher:
    def _make_fetcher(self, tmp_path: Path) -> RemoteCodeFetcher:
        return RemoteCodeFetcher(
            cache_dir=str(tmp_path / "cache"),
            allow_private_ip=True,
        )

    def _make_tar_gz(self, dest: Path, files: dict[str, str]) -> Path:
        tar_path = dest / "pkg.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tf:
            for name, content in files.items():
                f_path = dest / name
                f_path.write_text(content)
                tf.add(f_path, arcname=name)
        return tar_path

    def test_validate_url_blocks_non_http(self, tmp_path):
        fetcher = self._make_fetcher(tmp_path)
        with pytest.raises(ArtifactError, match="scheme"):
            fetcher.validate_artifact_url("ftp://example.com/pkg.tar.gz")

    def test_validate_url_blocks_localhost(self, tmp_path):
        fetcher = RemoteCodeFetcher(
            cache_dir=str(tmp_path / "cache"),
            allow_private_ip=False,
        )
        with pytest.raises(ArtifactError, match="Blocked"):
            fetcher.validate_artifact_url("http://localhost/pkg.tar.gz")

    def test_validate_url_blocks_private_ip(self, tmp_path):
        fetcher = RemoteCodeFetcher(
            cache_dir=str(tmp_path / "cache"),
            allow_private_ip=False,
        )
        with pytest.raises(ArtifactError, match="Private IP"):
            fetcher.validate_artifact_url("http://192.168.1.100/pkg.tar.gz")

    def test_validate_url_allows_private_when_flag_set(self, tmp_path):
        fetcher = self._make_fetcher(tmp_path)
        # Should not raise
        fetcher.validate_artifact_url("http://192.168.1.100/pkg.tar.gz")

    def test_extract_path_traversal_rejected(self, tmp_path):
        fetcher = self._make_fetcher(tmp_path)
        bad_tar = tmp_path / "bad.tar.gz"
        with tarfile.open(bad_tar, "w:gz") as tf:
            info = tarfile.TarInfo(name="../evil.py")
            info.size = 5
            import io
            tf.addfile(info, io.BytesIO(b"evil!"))

        with pytest.raises(ArtifactError, match="Unsafe tar"):
            fetcher._extract(bad_tar, tmp_path / "dest")

    def test_extract_valid_tar(self, tmp_path):
        fetcher = self._make_fetcher(tmp_path)
        tar_path = self._make_tar_gz(tmp_path, {"main.py": "print('hello')"})
        dest = tmp_path / "dest"
        pkg_dir = fetcher._extract(tar_path, dest)
        assert (pkg_dir / "main.py").exists()

    def test_sha256_mismatch_raises(self, tmp_path):
        fetcher = self._make_fetcher(tmp_path)
        tar_path = self._make_tar_gz(tmp_path, {"f.py": "x"})
        tar_bytes = tar_path.read_bytes()

        with patch("httpx.stream") as mock_stream:
            mock_resp = MagicMock()
            mock_resp.raise_for_status = MagicMock()
            mock_resp.iter_bytes.return_value = [tar_bytes]
            mock_stream.return_value.__enter__.return_value = mock_resp
            with pytest.raises(ArtifactError, match="SHA-256 mismatch"):
                fetcher._download(str(tar_path), "a" * 64)

    def test_cache_key_deterministic(self, tmp_path):
        fetcher = self._make_fetcher(tmp_path)
        k1 = fetcher._cache_key("http://x/y.tgz", "sha1")
        k2 = fetcher._cache_key("http://x/y.tgz", "sha1")
        k3 = fetcher._cache_key("http://x/y.tgz", "sha2")
        assert k1 == k2
        assert k1 != k3


# ===========================================================================
# NodeRegistry tests
# ===========================================================================

class TestNodeRegistry:
    def _make_redis(self) -> MagicMock:
        r = AsyncMock()
        r._store: dict = {}

        async def fake_set(key, val):
            r._store[key] = val

        async def fake_get(key):
            return r._store.get(key)

        async def fake_sadd(key, val):
            r._store.setdefault(key, set()).add(val)

        async def fake_smembers(key):
            return r._store.get(key, set())

        async def fake_zadd(key, mapping):
            pass

        async def fake_zrem(key, member):
            pass

        async def fake_delete(key):
            r._store.pop(key, None)

        async def fake_zrangebyscore(key, mn, mx):
            return []

        r.set = fake_set
        r.get = fake_get
        r.sadd = fake_sadd
        r.smembers = fake_smembers
        r.zadd = fake_zadd
        r.zrem = fake_zrem
        r.delete = fake_delete
        r.zrangebyscore = fake_zrangebyscore
        return r

    @pytest.mark.asyncio
    async def test_register_and_get(self):
        from src.platform.node_registry import NodeRegistry
        redis = self._make_redis()
        reg = NodeRegistry(redis)

        node = NodeInfo(node_id="n1", host="10.0.0.1")
        await reg.register(node)
        fetched = await reg.get("n1")
        assert fetched is not None
        assert fetched.node_id == "n1"
        assert fetched.host == "10.0.0.1"

    @pytest.mark.asyncio
    async def test_get_missing_returns_none(self):
        from src.platform.node_registry import NodeRegistry
        redis = self._make_redis()
        reg = NodeRegistry(redis)
        assert await reg.get("nonexistent") is None

    @pytest.mark.asyncio
    async def test_list_all(self):
        from src.platform.node_registry import NodeRegistry
        redis = self._make_redis()
        reg = NodeRegistry(redis)

        await reg.register(NodeInfo(node_id="n1", host="10.0.0.1"))
        await reg.register(NodeInfo(node_id="n2", host="10.0.0.2"))
        nodes = await reg.list_all()
        node_ids = {n.node_id for n in nodes}
        assert {"n1", "n2"} == node_ids

    @pytest.mark.asyncio
    async def test_detect_dead_nodes_empty(self):
        from src.platform.node_registry import NodeRegistry
        redis = self._make_redis()
        reg = NodeRegistry(redis)
        dead = await reg.detect_dead_nodes(timeout_seconds=60.0)
        assert dead == []
