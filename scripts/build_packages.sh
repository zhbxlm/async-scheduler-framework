#!/usr/bin/env bash
# scripts/build_packages.sh
# Build all user-facing sub-packages in dependency order.
# Output .whl files appear in dist/packages/<name>/
#
# Packages:
#   sdk     → async-scheduler-sdk     (HTTP client + CLI, deps: httpx, pydantic)
#   worker  → async-scheduler-worker  (BaseWorker, zero required deps)
#   proxy   → async-scheduler-proxy   (AsyncCommandProxy, deps: redis)
#   agent   → async-scheduler-agent   (Node Agent service, full deps)
#
# Usage:
#   ./scripts/build_packages.sh          # build all
#   ./scripts/build_packages.sh sdk      # build single package
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_ROOT="${REPO_ROOT}/dist/packages"
# Build order: lightweight → heavy
PACKAGES=(sdk worker proxy agent)

TARGET="${1:-all}"

log() { echo "[build] $*"; }
err() { echo "[build] ERROR: $*" >&2; exit 1; }

build_pkg() {
    local name="$1"
    local pkg_dir="${REPO_ROOT}/packages/${name}"
    local out_dir="${DIST_ROOT}/${name}"

    [[ -d "$pkg_dir" ]] || err "Package directory not found: $pkg_dir"
    mkdir -p "$out_dir"

    log "Building async-scheduler-${name} ..."
    python3 -m build --wheel --outdir "$out_dir" "$pkg_dir"
    local whl
    whl="$(ls "${out_dir}"/*.whl 2>/dev/null | tail -1)"
    log "  → $(basename "$whl")"
}

python3 -m pip install --quiet build

if [[ "$TARGET" == "all" ]]; then
    for pkg in "${PACKAGES[@]}"; do
        build_pkg "$pkg"
    done
    log ""
    log "All packages built successfully:"
    find "$DIST_ROOT" -name "*.whl" | sort | while read -r whl; do
        log "  $(basename "$whl")"
    done
else
    build_pkg "$TARGET"
fi
