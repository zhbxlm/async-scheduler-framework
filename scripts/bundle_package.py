#!/usr/bin/env python3
"""
Bundle script for standalone package wheels (Plan A).

Usage:
    python scripts/bundle_package.py task-api
    python scripts/bundle_package.py ops-api
    python scripts/bundle_package.py control-plane
    python scripts/bundle_package.py agent

What it does:
  1. Reads packages/<svc>/bundle.txt (list of src/* modules to include)
  2. Copies src/<module> → packages/<svc>/src/<pkg>/<module>  (flat mirror)
     with imports rewritten: from src.X → from <pkg>.X
  3. Runs `pip wheel` inside packages/<svc>/
  4. Cleans up the copied files after wheel is built

Source files are NEVER permanently duplicated — the copy lives only during the
build step and is cleaned up immediately after.
"""
from __future__ import annotations

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

# Per-service list of src/* subtrees to bundle
BUNDLE_MANIFEST: dict[str, list[str]] = {
    "task-api": [
        "common/async_db.py",
        "common/db_utils.py",
        "common/error_handling.py",
        "common/metrics.py",
        "common/redis_ha.py",
        "common/transaction.py",
        "common/ttl_constants.py",
        "models/callback_outbox.py",
        "models/dag_run.py",
        "models/task.py",
        "models/task_event.py",
        "models/task_run.py",
        "monitoring/metrics.py",
        "platform/circuit_breaker.py",
        "platform/queue_manager.py",
        "platform/task_completion_node.py",
        "platform/task_creator.py",
        "platform/task_state_machine.py",
        "services/run_tracking.py",
        "services/task_application.py",
        "services/task_timeline.py",
        "services/task_validation.py",
    ],
    "ops-api": [
        "common/async_db.py",
        "common/db_utils.py",
        "common/error_handling.py",
        "common/metrics.py",
        "common/redis_ha.py",
        "common/service_logger.py",
        "common/ttl_constants.py",
        "models/callback_outbox.py",
        "models/capability.py",
        "models/cluster.py",
        "models/dag_run.py",
        "models/node.py",
        "models/operator_action.py",
        "models/task.py",
        "models/task_event.py",
        "models/task_run.py",
        "monitoring/alerts.py",
        "monitoring/metrics.py",
        "platform/base_registry.py",
        "platform/capability_registry.py",
        "platform/circuit_breaker.py",
        "platform/cluster_registry.py",
        "platform/dag_loader.py",
        "platform/node_registry.py",
        "platform/queue_manager.py",
        "services/access_policy.py",
        "services/callback_ops.py",
        "services/callback_replay_policy.py",
        "services/force_operations.py",
        "services/governance.py",
        "services/operator_actions.py",
        "services/operator_dashboard.py",
        "services/operator_queries.py",
        "services/operator_ux.py",
        "services/recovery_explainer.py",
        "services/replay_chain_queries.py",
        "services/replay_lineage.py",
        "services/replay_policy.py",
        "services/resource_application.py",
        "services/run_centric_queries.py",
        "services/run_tracking.py",
        "services/task_audit_queries.py",
        "services/task_timeline.py",
    ],
    "control-plane": [
        "common/async_db.py",
        "common/db_utils.py",
        "common/error_handling.py",
        "common/lifecycle.py",
        "common/metrics.py",
        "common/redis_client.py",
        "common/redis_ha.py",
        "common/transaction.py",
        "common/ttl_constants.py",
        "models/callback_outbox.py",
        "models/dag_run.py",
        "models/task.py",
        "models/task_event.py",
        "models/task_run.py",
        "monitoring/metrics.py",
        "platform/base_registry.py",
        "platform/circuit_breaker.py",
        "platform/cron_scheduler.py",
        "platform/queue_manager.py",
        "platform/schedule_registry.py",
        "platform/task_completion_node.py",
        "platform/task_creator.py",
        "platform/task_reconciler.py",
        "platform/task_state_machine.py",
        "platform/tenant_registry.py",
        "services/callback_dispatcher.py",
        "services/compensation.py",
        "services/run_tracking.py",
        "services/task_timeline.py",
    ],
    "agent": [
        "models/deploy.py",
        "models/node.py",
        "platform/remote_code_fetcher.py",
    ],
}


def rewrite_imports(text: str, pkg: str) -> str:
    """Rewrite `from src.X` → `from <pkg>.X` inside bundled files."""
    return re.sub(r"from src\.", f"from {pkg}.", text)


def bundle(svc: str, *, build_wheel: bool = True, clean: bool = True) -> None:
    pkg  = PKG_MAP[svc]
    svc_dir  = REPO_ROOT / "packages" / svc
    pkg_dir  = svc_dir / "src" / pkg
    manifest = BUNDLE_MANIFEST.get(svc, [])

    copied: list[Path] = []
    print(f"[bundle] {svc} → {pkg}  ({len(manifest)} files to bundle)")

    try:
        # Step 1: copy & rewrite
        for rel in manifest:
            src = SRC_ROOT / rel
            dst = pkg_dir / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            text = rewrite_imports(src.read_text(), pkg)
            dst.write_text(text)
            copied.append(dst)

        # Step 2: build wheel
        if build_wheel:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "wheel", ".", "--no-deps", "-w", "dist/"],
                cwd=svc_dir,
                check=True,
            )

        print(f"[bundle] {svc} done")

    finally:
        # Step 3: clean up bundled copies
        if clean:
            for f in copied:
                if f.exists():
                    f.unlink()
            # remove empty __pycache__ dirs
            for d in pkg_dir.rglob("__pycache__"):
                if not any(d.iterdir()):
                    d.rmdir()
            print(f"[bundle] cleaned {len(copied)} temp files")


if __name__ == "__main__":
    services = sys.argv[1:] or list(PKG_MAP.keys())
    for svc in services:
        if svc not in PKG_MAP:
            print(f"Unknown service: {svc}. Available: {list(PKG_MAP.keys())}")
            sys.exit(1)
        bundle(svc, build_wheel=("--no-wheel" not in sys.argv))
