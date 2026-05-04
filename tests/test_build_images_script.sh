#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

out="$(bash scripts/build_images.sh --dry-run)"
printf '%s\n' "$out" | grep -q '\[dry-run\] docker build'
printf '%s\n' "$out" | grep -q 'async-scheduler-framework:latest'

if bash scripts/build_images.sh --check >/tmp/build-check.out 2>&1; then
  grep -Eq 'docker available|docker not available' /tmp/build-check.out
else
  grep -Eq 'docker available|docker not available' /tmp/build-check.out
fi

echo 'build image script checks passed'
