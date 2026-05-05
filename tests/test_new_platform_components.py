"""Tests for RemoteCodeFetcher and NodeRegistry."""
from __future__ import annotations

import tarfile
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.models.node import NodeInfo
from src.platform.remote_code_fetcher import ArtifactError, RemoteCodeFetcher


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
        from unittest.mock import AsyncMock
        r = AsyncMock()
        r._store: dict = {}

        async def fake_set(key, val):
            r._store[key] = val

        async def fake_get(key):
            def fake_register_script(script):
                return AsyncMock()
            r.register_script = fake_register_script
            return r._store.get(key)

        async def fake_sadd(key, val):
            r._store.setdefault(key, set()).add(val)

        async def fake_smembers(key):
            def fake_register_script(script):
                return AsyncMock()
            r.register_script = fake_register_script
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

        # Pipeline mock — needed by batch-optimised list_all / list_due
        class _FakePipeline:
            def __init__(self):
                self.commands = []
            def get(self, key):
                self.commands.append(('get', key))
                return self
            async def execute(self):
                result = []
                for cmd, key in self.commands:
                    if cmd == 'get':
                        result.append(r._store.get(key))
                    else:
                        result.append(None)
                self.commands = []
                def fake_register_script(script):
                    return AsyncMock()
                r.register_script = fake_register_script
                return result

        def fake_pipeline():
            return _FakePipeline()
        r.pipeline = fake_pipeline

        def fake_register_script(script):
            return AsyncMock()
        r.register_script = fake_register_script
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