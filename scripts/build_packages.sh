#!/usr/bin/env bash
# scripts/build_packages.sh — build all 7 user-facing sub-packages
#
# Packages (lightest → heaviest):
#   sdk       async-scheduler-sdk       HTTP client (httpx, pydantic)
#   worker    async-scheduler-worker    BaseWorker (zero deps)
#   proxy     async-scheduler-proxy     Redis dispatch proxy (redis)
#   cli       async-scheduler-cli       kubectl-style CLI (httpx, click)
#   task-api  async-scheduler-task-api  Task submission service (full stack)
#   ops-api   async-scheduler-ops-api   Ops/Admin service (full stack + Ray)
#   agent     async-scheduler-agent     Worker node service (full stack + Ray)
#
# Usage:
#   ./scripts/build_packages.sh           # build all
#   ./scripts/build_packages.sh sdk       # build single package
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_ROOT="${REPO_ROOT}/dist/packages"
PACKAGES=(sdk worker proxy cli task-api ops-api agent)

TARGET="${1:-all}"

log() { echo "[build] $*"; }
err() { echo "[build] ERROR: $*" >&2; exit 1; }

build_pkg() {
    local name="$1"
    local pkg_dir="${REPO_ROOT}/packages/${name}"
    local out_dir="${DIST_ROOT}/${name}"
    [[ -d "$pkg_dir" ]] || err "Not found: $pkg_dir"
    mkdir -p "$out_dir"
    log "Building async-scheduler-${name} ..."
    python3 -m build --wheel --outdir "$out_dir" "$pkg_dir"
    log "  → $(basename "$(ls "${out_dir}"/*.whl 2>/dev/null | tail -1)")"
}

python3 -m pip install --quiet build

if [[ "$TARGET" == "all" ]]; then
    for pkg in "${PACKAGES[@]}"; do build_pkg "$pkg"; done
    log ""
    log "All packages built:"
    find "$DIST_ROOT" -name "*.whl" | sort | while read -r w; do log "  $(basename "$w")"; done
else
    build_pkg "$TARGET"
fi
