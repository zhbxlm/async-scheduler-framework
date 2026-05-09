#!/usr/bin/env python3
"""Centralized version management for async-scheduler packages.

Usage:
  python scripts/release_version.py show
  python scripts/release_version.py set 1.2.1 [--tag]
"""
from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION_FILE = ROOT / "VERSION"
PACKAGE_PYPROJECTS = [
    ROOT / "packages" / "runtime-core" / "pyproject.toml",
    ROOT / "packages" / "task-api" / "pyproject.toml",
    ROOT / "packages" / "ops-api" / "pyproject.toml",
    ROOT / "packages" / "control-plane" / "pyproject.toml",
    ROOT / "packages" / "agent" / "pyproject.toml",
]


def read_version() -> str:
    return VERSION_FILE.read_text().strip()


def write_version(version: str) -> None:
    VERSION_FILE.write_text(f"{version}\n")


VERSION_RE = re.compile(r'^(version\s*=\s*")([^"]+)(")$', re.M)
RUNTIME_CORE_REQ_RE = re.compile(r'(async-scheduler-runtime-core>=)([^\]\s",]+)')


def update_pyproject(path: Path, version: str) -> None:
    text = path.read_text()
    text, count = VERSION_RE.subn(rf'\g<1>{version}\3', text, count=1)
    if count != 1:
        raise RuntimeError(f"failed to update version in {path}")
    text = RUNTIME_CORE_REQ_RE.sub(rf'\g<1>{version}', text)
    path.write_text(text)


def git_tag(version: str) -> None:
    subprocess.run(["git", "tag", f"v{version}"], cwd=ROOT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("show")
    set_p = sub.add_parser("set")
    set_p.add_argument("version")
    set_p.add_argument("--tag", action="store_true")

    args = parser.parse_args()

    if args.cmd == "show":
        print(read_version())
        return

    version = args.version
    write_version(version)
    for path in PACKAGE_PYPROJECTS:
        update_pyproject(path, version)

    print(f"updated version to {version}")
    if args.tag:
        git_tag(version)
        print(f"created git tag v{version}")


if __name__ == "__main__":
    main()
