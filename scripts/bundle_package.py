#!/usr/bin/env python3
"""
Bundle script for standalone package wheels (Plan A).

Usage:
    python scripts/bundle_package.py [task-api] [ops-api] [control-plane] [agent]
    python scripts/bundle_package.py --manifest-only   # just print manifest, no build

What it does:
  1. Statically analyses src/<pkg>/ import graph to find all src.* dependencies
  2. Copies those src/* files into packages/<svc>/src/<pkg>/ with imports rewritten
     using AST-safe rewriting (not naive regex)
  3. Also copies the glue-layer's own infra/ subdirectory (not reachable via src.*)
  4. Runs `pip wheel` inside packages/<svc>/
  5. Cleans up all copied files immediately after (source never permanently duplicated)

Manifest is auto-generated from AST analysis on every run — never hand-written.
"""
from __future__ import annotations

import ast
import re
import shutil
import subprocess
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

# ─────────────────────────────────────────────────────────────────────────────
# AST-based import rewriter (replaces naive regex)
# ─────────────────────────────────────────────────────────────────────────────

class _ImportRewriter(ast.NodeTransformer):
    """Rewrite `from src.X` → `from <pkg>.X` and `import src.X` → `import <pkg>.X`."""

    def __init__(self, pkg: str) -> None:
        self._pkg = pkg

    def visit_ImportFrom(self, node: ast.ImportFrom) -> ast.ImportFrom:
        if node.module and node.module.startswith("src."):
            node.module = self._pkg + node.module[3:]  # replace "src" prefix
        return node

    def visit_Import(self, node: ast.Import) -> ast.Import:
        for alias in node.names:
            if alias.name.startswith("src."):
                alias.name = self._pkg + alias.name[3:]
        return node


def rewrite_imports(source: str, pkg: str) -> str:
    """Rewrite src.* imports in *source* using AST transformation."""
    try:
        tree = ast.parse(source)
        new_tree = _ImportRewriter(pkg).visit(tree)
        ast.fix_missing_locations(new_tree)
        return ast.unparse(new_tree)
    except SyntaxError:
        # fallback to regex for files that can't be parsed (shouldn't happen)
        return re.sub(r"\bsrc\.", f"{pkg}.", source)


# ─────────────────────────────────────────────────────────────────────────────
# Static dependency analysis
# ─────────────────────────────────────────────────────────────────────────────

def _get_src_deps(filepath: Path) -> set[str]:
    """Return set of src-relative paths this file depends on via src.* imports."""
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
            # from src.X.Y import Z  → src/X/Y.py
            candidate = SRC_ROOT / Path(*parts[1:])
            py_file = candidate.with_suffix(".py")
            if py_file.exists():
                deps.add(str(py_file.relative_to(SRC_ROOT)))
            # package __init__
            init = candidate / "__init__.py"
            if init.exists():
                deps.add(str(init.relative_to(SRC_ROOT)))
            # from src.X import Y where Y is a submodule
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


def compute_manifest(entry_dir: str) -> list[str]:
    """Compute the minimal set of src/* files needed by *entry_dir* (BFS)."""
    visited: set[Path] = set()
    bundle:  set[str]  = set()
    queue:   list[Path] = []

    entry = Path(entry_dir)
    if entry.exists():
        queue.extend(f for f in entry.rglob("*.py") if "__pycache__" not in str(f))

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

    # exclude bare __init__.py files (package markers, not real code)
    return sorted(r for r in bundle if not r.endswith("/__init__.py"))


# ─────────────────────────────────────────────────────────────────────────────
# Bundle entry point
# ─────────────────────────────────────────────────────────────────────────────

def bundle(svc: str, *, build_wheel: bool = True, clean: bool = True) -> None:
    pkg     = PKG_MAP[svc]
    svc_dir = REPO_ROOT / "packages" / svc
    pkg_dir = svc_dir / "src" / pkg

    # 1. Compute manifest via static analysis
    manifest = compute_manifest(str(REPO_ROOT / "src" / pkg))
    print(f"[bundle] {svc} → {pkg}  ({len(manifest)} src files in manifest)")

    copied: list[Path] = []

    try:
        # 2a. Copy & rewrite src/* dependencies
        for rel in manifest:
            src = SRC_ROOT / rel
            dst = pkg_dir / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            rewritten = rewrite_imports(src.read_text(), pkg)
            dst.write_text(rewritten)
            copied.append(dst)

        # 2b. Copy glue-layer infra/ (not reachable via src.* imports)
        glue_infra = REPO_ROOT / "src" / pkg / "infra"
        if glue_infra.exists():
            for src_f in sorted(glue_infra.rglob("*.py")):
                if "__pycache__" in str(src_f):
                    continue
                rel_to_pkg = src_f.relative_to(REPO_ROOT / "src" / pkg)
                dst = pkg_dir / rel_to_pkg
                dst.parent.mkdir(parents=True, exist_ok=True)
                # infra files reference scheduler_xxx.* (already correct namespace)
                dst.write_text(src_f.read_text())
                copied.append(dst)
            print(f"[bundle] {svc}: copied infra/ ({sum(1 for f in copied if 'infra' in str(f))} files)")

        # 3. Build wheel
        if build_wheel:
            subprocess.run(
                [sys.executable, "-m", "pip", "wheel", ".", "--no-deps", "-w", "dist/"],
                cwd=svc_dir,
                check=True,
            )

        print(f"[bundle] {svc} done ✓")

    finally:
        # 4. Clean up
        if clean:
            for f in copied:
                if f.exists():
                    f.unlink()
            # remove empty dirs created during copy
            for d in sorted(pkg_dir.rglob("*"), reverse=True):
                if d.is_dir():
                    try:
                        d.rmdir()
                    except OSError:
                        pass
            print(f"[bundle] {svc}: cleaned {len(copied)} temp files")


def print_manifest(svc: str) -> None:
    pkg = PKG_MAP[svc]
    manifest = compute_manifest(str(REPO_ROOT / "src" / pkg))
    print(f"\n=== {svc} ({len(manifest)} files) ===")
    for f in manifest:
        print(f"  {f}")

    # also list infra/ files
    glue_infra = REPO_ROOT / "src" / pkg / "infra"
    if glue_infra.exists():
        infra_files = [f for f in glue_infra.rglob("*.py") if "__pycache__" not in str(f)]
        print(f"  [infra/ {len(infra_files)} files — bundled separately]")
        for f in sorted(infra_files):
            print(f"    {f.relative_to(REPO_ROOT / 'src' / pkg)}")


if __name__ == "__main__":
    args = sys.argv[1:]
    manifest_only = "--manifest-only" in args
    args = [a for a in args if not a.startswith("--")]

    services = args or list(PKG_MAP.keys())
    for svc in services:
        if svc not in PKG_MAP:
            print(f"Unknown service: {svc}. Available: {list(PKG_MAP.keys())}")
            sys.exit(1)
        if manifest_only:
            print_manifest(svc)
        else:
            bundle(svc, build_wheel=True)
