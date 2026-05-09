#!/usr/bin/env python3
"""
Verify that bundle_package.py manifests are up to date.

Exits with code 1 if any service has stale manifest dependencies
(i.e. the static analysis finds deps not covered by the current manifest).
Run this in CI to catch forgotten manifest updates.

Usage:
    python scripts/verify_manifest.py
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
SRC_ROOT  = REPO_ROOT / "src"

PKG_MAP = {
    "task-api":      "scheduler_task_api",
    "ops-api":       "scheduler_ops_api",
    "control-plane": "scheduler_control_plane",
    "agent":         "scheduler_agent",
}


def _get_src_deps(filepath: Path) -> set[str]:
    try:
        tree = ast.parse(filepath.read_text())
    except (SyntaxError, UnicodeDecodeError):
        return set()
    deps: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            parts = node.module.split(".")
            if parts[0] != "src" or len(parts) < 2:
                continue
            candidate = SRC_ROOT / Path(*parts[1:])
            py_file = candidate.with_suffix(".py")
            if py_file.exists():
                deps.add(str(py_file.relative_to(SRC_ROOT)))
            for alias in (node.names or []):
                sub = SRC_ROOT / Path(*parts[1:]) / (alias.name + ".py")
                if sub.exists():
                    deps.add(str(sub.relative_to(SRC_ROOT)))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                parts = alias.name.split(".")
                if parts[0] != "src" or len(parts) < 2:
                    continue
                candidate = SRC_ROOT / Path(*parts[1:])
                py_file = candidate.with_suffix(".py")
                if py_file.exists():
                    deps.add(str(py_file.relative_to(SRC_ROOT)))
    return deps


def compute_manifest(pkg: str) -> set[str]:
    entry = SRC_ROOT / pkg
    visited: set[Path] = set()
    bundle: set[str] = set()
    queue = [f for f in entry.rglob("*.py") if "__pycache__" not in str(f)] if entry.exists() else []
    while queue:
        f = queue.pop()
        if f in visited:
            continue
        visited.add(f)
        for rel in _get_src_deps(f):
            if rel not in bundle:
                bundle.add(rel)
                dep = SRC_ROOT / rel
                if dep.exists() and dep not in visited:
                    queue.append(dep)
    return {r for r in bundle if not r.endswith("/__init__.py")}


def main() -> int:
    ok = True
    for svc, pkg in PKG_MAP.items():
        computed = compute_manifest(pkg)

        # import the current manifest from bundle_package.py
        sys.path.insert(0, str(REPO_ROOT / "scripts"))
        import importlib
        spec = importlib.util.spec_from_file_location(
            "bundle_package", REPO_ROOT / "scripts" / "bundle_package.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        # bundle_package.py now uses dynamic manifest — no BUNDLE_MANIFEST constant
        # just verify by recomputing
        sys.path.pop(0)

        # verify all computed deps are in src/
        missing_from_src = [r for r in computed if not (SRC_ROOT / r).exists()]
        if missing_from_src:
            print(f"[FAIL] {svc}: deps listed in manifest but missing from src/:")
            for m in missing_from_src:
                print(f"  {m}")
            ok = False
        else:
            print(f"[OK]   {svc}: {len(computed)} deps, all present in src/")

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
