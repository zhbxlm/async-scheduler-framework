"""DeployManager — download, verify, and unpack deployment packages.

Aligned with docs/deepwiki-reference/节点代理.md (deploy section).
"""
from __future__ import annotations
import hashlib
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)
_DEPLOY_BASE = os.getenv("AGENT_DEPLOY_BASE_DIR", "/opt/ray-amu/deploys")
_DOWNLOAD_TIMEOUT = int(os.getenv("AGENT_DEPLOY_DOWNLOAD_TIMEOUT", "600"))
_MAX_PACKAGES = int(os.getenv("AGENT_DEPLOY_MAX_PACKAGES", "20"))


class DeployManager:
    def __init__(self, base_dir: str = _DEPLOY_BASE) -> None:
        self._base = Path(base_dir)
        self._base.mkdir(parents=True, exist_ok=True)

    async def deploy(self, package_id: str, url: str, sha256: str | None = None) -> dict:
        dest = self._base / package_id
        if dest.exists():
            logger.info("DeployManager: %s already deployed", package_id)
            return {"status": "cached", "path": str(dest)}

        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            await self._download(url, tmp_path)
            if sha256:
                self._verify_sha256(tmp_path, sha256)
            self._safe_extract(tmp_path, dest)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

        logger.info("DeployManager: deployed %s -> %s", package_id, dest)
        return {"status": "deployed", "path": str(dest)}

    async def undeploy(self, package_id: str) -> bool:
        dest = self._base / package_id
        if not dest.exists():
            return True
        import shutil
        shutil.rmtree(dest)
        logger.info("DeployManager: undeployed %s", package_id)
        return True

    async def _download(self, url: str, dest_path: str) -> None:
        async with httpx.AsyncClient(timeout=_DOWNLOAD_TIMEOUT) as client:
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                with open(dest_path, "wb") as f:
                    async for chunk in resp.aiter_bytes():
                        f.write(chunk)

    def _verify_sha256(self, path: str, expected: str) -> None:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        actual = h.hexdigest()
        if actual != expected:
            raise ValueError(f"SHA-256 mismatch: expected {expected}, got {actual}")

    def _safe_extract(self, archive: str, dest: Path) -> None:
        import tarfile
        dest.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive, "r:gz") as tf:
            for member in tf.getmembers():
                member_path = dest / member.name
                if not str(member_path.resolve()).startswith(str(dest.resolve())):
                    raise ValueError(f"Path traversal detected: {member.name}")
                if member.issym() or member.islnk():
                    raise ValueError(f"Symlink not allowed: {member.name}")
            tf.extractall(dest)
