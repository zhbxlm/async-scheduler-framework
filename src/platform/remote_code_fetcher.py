"""RemoteCodeFetcher — aligned with docs/deepwiki-reference/RayData 集成.md

Handles download, SHA-256 verification, extraction and LRU caching
of remote code packages (tar.gz artifacts) used by RayDataClient and
SchedulerActor.

Security measures implemented:
- URL allowlist (http/https only, no private IPs unless overridden)
- SHA-256 checksum verification
- Tar path-traversal protection (no absolute paths, no "..")
- setuid/setgid bit stripping
- Symlink exfiltration check
- Configurable size limit
- LRU cache eviction by mtime
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import tarfile
import tempfile
import urllib.parse
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

# Default configuration (can be overridden via constructor)
_DEFAULT_CACHE_DIR = "/tmp/artifact_cache"
_DEFAULT_MAX_SIZE_MB = 1024
_DEFAULT_MAX_CACHE_ENTRIES = 50

# Blocked hostnames / patterns
_BLOCKED_HOSTNAMES = frozenset({"localhost", "127.0.0.1", "::1", "0.0.0.0"})
_PRIVATE_IP_PATTERNS = re.compile(
    r"^(10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.|169\.254\.)"
)


class ArtifactError(Exception):
    """Raised for any artifact validation / fetch failure."""


class RemoteCodeFetcher:
    """Download, verify and cache remote code packages.

    Parameters
    ----------
    cache_dir:
        Local directory used as the artifact cache root.
    max_size_mb:
        Maximum artifact size in MB.  Default 1024.
    max_cache_entries:
        Maximum number of cache entries before LRU eviction.  Default 50.
    allow_private_ip:
        If True, private / loopback IP addresses are allowed as download
        targets.  Controlled by env var ``CALLBACK_ALLOW_PRIVATE_IP=true``.
    """

    def __init__(
        self,
        *,
        cache_dir: str = _DEFAULT_CACHE_DIR,
        max_size_mb: int = _DEFAULT_MAX_SIZE_MB,
        max_cache_entries: int = _DEFAULT_MAX_CACHE_ENTRIES,
        allow_private_ip: bool | None = None,
    ) -> None:
        self._cache_dir = Path(cache_dir)
        self._max_size_bytes = max_size_mb * 1024 * 1024
        self._max_cache_entries = max_cache_entries
        if allow_private_ip is None:
            allow_private_ip = os.environ.get("CALLBACK_ALLOW_PRIVATE_IP", "").lower() in (
                "true", "1", "yes"
            )
        self._allow_private_ip = allow_private_ip
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_artifact(
        self, artifact_url: str, expected_sha256: str | None = None
    ) -> str:
        """Ensure *artifact_url* is present locally and return its directory.

        Returns the path to the extracted package directory.  Raises
        :class:`ArtifactError` on any validation or download failure.
        """
        self.validate_artifact_url(artifact_url)

        cache_key = self._cache_key(artifact_url, expected_sha256)
        cache_path = self._cache_dir / cache_key
        ready_marker = cache_path / ".ready"

        if ready_marker.exists() and (cache_path / "package").is_dir():
            logger.debug("RemoteCodeFetcher: cache hit key=%s", cache_key)
            return str(cache_path / "package")

        # GC before downloading
        self._gc_artifact_cache()

        # Download
        tar_path = self._download(artifact_url, expected_sha256)
        try:
            pkg_dir = self._extract(tar_path, cache_path)
            ready_marker.touch()
            return str(pkg_dir)
        finally:
            tar_path.unlink(missing_ok=True)

    def validate_artifact_url(self, url: str) -> None:
        """Validate *url* is safe to download from.

        Raises :class:`ArtifactError` if the URL fails validation.
        """
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ArtifactError(f"Unsupported URL scheme: {parsed.scheme!r}")
        host = parsed.hostname or ""
        if not host:
            raise ArtifactError("URL has no hostname")
        if host in _BLOCKED_HOSTNAMES:
            raise ArtifactError(f"Blocked hostname: {host!r}")
        if not self._allow_private_ip and _PRIVATE_IP_PATTERNS.match(host):
            raise ArtifactError(
                f"Private IP address not allowed: {host!r}. "
                "Set CALLBACK_ALLOW_PRIVATE_IP=true to override."
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _download(self, url: str, expected_sha256: str | None) -> Path:
        """Stream-download *url* to a temp file, verify checksum, return path."""
        fd, tmp_path = tempfile.mkstemp(suffix=".tar.gz")
        tmp = Path(tmp_path)
        sha256 = hashlib.sha256()
        downloaded = 0

        try:
            with httpx.stream("GET", url, follow_redirects=True, timeout=60) as r:
                r.raise_for_status()
                with os.fdopen(fd, "wb") as f:
                    for chunk in r.iter_bytes(chunk_size=65536):
                        downloaded += len(chunk)
                        if downloaded > self._max_size_bytes:
                            raise ArtifactError(
                                f"Artifact exceeds size limit "
                                f"({self._max_size_bytes // (1024*1024)} MB)"
                            )
                        sha256.update(chunk)
                        f.write(chunk)
        except ArtifactError:
            tmp.unlink(missing_ok=True)
            raise
        except Exception as exc:
            tmp.unlink(missing_ok=True)
            raise ArtifactError(f"Download failed: {exc}") from exc

        if expected_sha256:
            actual = sha256.hexdigest()
            if actual != expected_sha256.lower():
                tmp.unlink(missing_ok=True)
                raise ArtifactError(
                    f"SHA-256 mismatch: expected {expected_sha256}, got {actual}"
                )

        return tmp

    def _extract(self, tar_path: Path, dest: Path) -> Path:
        """Extract *tar_path* into *dest/package/*, return the package dir."""
        pkg_dir = dest / "package"
        pkg_dir.mkdir(parents=True, exist_ok=True)

        with tarfile.open(tar_path, "r:gz") as tf:
            for member in tf.getmembers():
                # Path-traversal protection
                if os.path.isabs(member.name) or ".." in Path(member.name).parts:
                    raise ArtifactError(
                        f"Unsafe tar entry: {member.name!r}"
                    )
                # Strip setuid/setgid bits
                member.mode = member.mode & ~(0o4000 | 0o2000)
                # Symlink safety
                if member.issym() or member.islnk():
                    target = Path(member.linkname)
                    if target.is_absolute() or ".." in target.parts:
                        raise ArtifactError(
                            f"Unsafe symlink in tar: {member.name!r} -> {member.linkname!r}"
                        )
            tf.extractall(pkg_dir)  # noqa: S202 (path checks done above)

        return pkg_dir

    def _gc_artifact_cache(self) -> int:
        """Remove oldest cache entries if over limit. Returns eviction count."""
        entries = [
            p for p in self._cache_dir.iterdir()
            if p.is_dir() and (p / ".ready").exists()
        ]
        if len(entries) < self._max_cache_entries:
            return 0
        entries.sort(key=lambda p: p.stat().st_mtime)
        to_remove = entries[: len(entries) - self._max_cache_entries + 1]
        for p in to_remove:
            shutil.rmtree(p, ignore_errors=True)
            logger.info("RemoteCodeFetcher: evicted cache entry %s", p.name)
        return len(to_remove)

    @staticmethod
    def _cache_key(url: str, sha256: str | None) -> str:
        raw = f"{url}:{sha256 or ''}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]
