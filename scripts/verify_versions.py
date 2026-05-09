#!/usr/bin/env python3
"""Verify all pyproject.toml version numbers match VERSION file.

Run in CI to catch cases where a package version was not updated during release.
Exits with code 1 if any mismatch is found.
"""
from __future__ import annotations
import sys
from pathlib import Path
try:
    import tomllib  # Python 3.11+
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

ROOT = Path(__file__).resolve().parents[1]
VERSION_FILE = ROOT / "VERSION"

PYPROJECTS = [
    ROOT / "pyproject.toml",
    ROOT / "packages" / "runtime-core" / "pyproject.toml",
    ROOT / "packages" / "task-api" / "pyproject.toml",
    ROOT / "packages" / "ops-api" / "pyproject.toml",
    ROOT / "packages" / "control-plane" / "pyproject.toml",
    ROOT / "packages" / "agent" / "pyproject.toml",
    ROOT / "packages" / "cli" / "pyproject.toml",
    ROOT / "packages" / "proxy" / "pyproject.toml",
    ROOT / "packages" / "sdk" / "pyproject.toml",
    ROOT / "packages" / "worker" / "pyproject.toml",
]


def main() -> int:
    canonical = VERSION_FILE.read_text().strip()
    print(f"Canonical version (VERSION file): {canonical}")
    ok = True
    for p in PYPROJECTS:
        if not p.exists():
            print(f"  SKIP {p.relative_to(ROOT)} (not found)")
            continue
        data = tomllib.loads(p.read_text())
        ver = data.get("project", {}).get("version", "?")
        rel = p.relative_to(ROOT)
        if ver == canonical:
            print(f"  ✓  {rel}: {ver}")
        else:
            print(f"  ✗  {rel}: {ver}  ← MISMATCH (expected {canonical})")
            ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
