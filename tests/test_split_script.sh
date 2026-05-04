#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

out="$(DATABASE_URL=mysql+asyncmy://u:p@db:3306/test REDIS_URL=redis://cache:6379/0 bash scripts/run_local_split.sh validate)"
printf '%s\n' "$out" | grep -q 'Configuration valid'
printf '%s\n' "$out" | grep -q 'deployment_role'

out2="$(DATABASE_URL=mysql+asyncmy://u:p@db:3306/test REDIS_URL=redis://cache:6379/0 bash scripts/run_local_split.sh dry-run)"
printf '%s\n' "$out2" | grep -q '\[dry-run\] api'
printf '%s\n' "$out2" | grep -q 'scheduler-reconciler'

echo 'split script checks passed'
