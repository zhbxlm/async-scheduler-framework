#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

ACTION="${1:-}"
export ENVIRONMENT="${ENVIRONMENT:-local}"
export DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-local-split}"
export BACKEND_QUEUE_TYPE="${BACKEND_QUEUE_TYPE:-${QUEUE_TYPE:-redis}}"
export BACKEND_LOCK_TYPE="${BACKEND_LOCK_TYPE:-${LOCK_TYPE:-redis}}"
export BACKEND_REGISTRY_TYPE="${BACKEND_REGISTRY_TYPE:-${REGISTRY_TYPE:-memory}}"

mkdir -p .run logs

require_env() {
  : "${DATABASE_URL:?DATABASE_URL is required}"
  : "${REDIS_URL:?REDIS_URL is required}"
}

print_config() {
  python3 - <<'PY'
from async_scheduler.settings import load_settings
s = load_settings(validate=True)
print("Configuration valid")
print(s.summary())
PY
}

start_service() {
  local name="$1"
  shift
  echo "[start] $name"
  nohup "$@" >"logs/${name}.log" 2>&1 &
  echo $! > ".run/${name}.pid"
}

show_dry_run() {
  echo "[dry-run] api => SERVICE_NAME=scheduler-api DEPLOYMENT_ROLE=api NODE_ID=
  async-scheduler api --host 0.0.0.0 --port 8000"
  echo "[dry-run] worker => SERVICE_NAME=scheduler-worker DEPLOYMENT_ROLE=worker NODE_ID=worker-local
  async-scheduler worker --workers ${WORKER_COUNT:-2} --max-concurrent ${MAX_CONCURRENT:-16}"
  echo "[dry-run] scheduler => SERVICE_NAME=scheduler-cron DEPLOYMENT_ROLE=scheduler NODE_ID=scheduler-local
  async-scheduler scheduler-service --poll-interval ${SCHEDULER_POLL_INTERVAL:-15}"
  echo "[dry-run] reconciler => SERVICE_NAME=scheduler-reconciler DEPLOYMENT_ROLE=reconciler NODE_ID=reconciler-local
  async-scheduler reconciler-service --interval ${RECONCILER_INTERVAL:-20}"
}

stop_service() {
  local name="$1"
  if [ -f ".run/${name}.pid" ]; then
    local pid
    pid="$(cat ".run/${name}.pid")"
    if kill -0 "$pid" 2>/dev/null; then
      echo "[stop] $name ($pid)"
      kill "$pid" || true
    fi
    rm -f ".run/${name}.pid"
  fi
}

case "$ACTION" in
  validate)
    require_env
    print_config
    ;;
  dry-run)
    require_env
    print_config >/dev/null
    show_dry_run
    ;;
  start)
    require_env
    print_config
    python -m pip install -e . >/dev/null
    async-scheduler init-db-cmd || true
    start_service api env SERVICE_NAME=scheduler-api DEPLOYMENT_ROLE=api async-scheduler api --host 0.0.0.0 --port 8000
    start_service worker env SERVICE_NAME=scheduler-worker DEPLOYMENT_ROLE=worker NODE_ID=worker-local async-scheduler worker --workers "${WORKER_COUNT:-2}" --max-concurrent "${MAX_CONCURRENT:-16}"
    start_service scheduler env SERVICE_NAME=scheduler-cron DEPLOYMENT_ROLE=scheduler NODE_ID=scheduler-local async-scheduler scheduler-service --poll-interval "${SCHEDULER_POLL_INTERVAL:-15}"
    start_service reconciler env SERVICE_NAME=scheduler-reconciler DEPLOYMENT_ROLE=reconciler NODE_ID=reconciler-local async-scheduler reconciler-service --interval "${RECONCILER_INTERVAL:-20}"
    echo "All services started. Logs: ./logs/*.log"
    ;;
  stop)
    stop_service api
    stop_service worker
    stop_service scheduler
    stop_service reconciler
    ;;
  restart)
    "$0" stop
    sleep 1
    "$0" start
    ;;
  status)
    for svc in api worker scheduler reconciler; do
      if [ -f ".run/${svc}.pid" ] && kill -0 "$(cat ".run/${svc}.pid")" 2>/dev/null; then
        echo "$svc: RUNNING pid=$(cat ".run/${svc}.pid")"
      else
        echo "$svc: STOPPED"
      fi
    done
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|validate|dry-run}"
    exit 1
    ;;
esac
