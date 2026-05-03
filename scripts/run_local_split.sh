#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

: "${DATABASE_URL:?DATABASE_URL is required}"
: "${REDIS_URL:?REDIS_URL is required}"

mkdir -p .run logs

start_service() {
  local name="$1"
  shift
  echo "[start] $name"
  nohup "$@" >"logs/${name}.log" 2>&1 &
  echo $! > ".run/${name}.pid"
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

case "${1:-}" in
  start)
    python -m pip install -e . >/dev/null
    async-scheduler init-db-cmd || true
    start_service api env SERVICE_NAME=scheduler-api async-scheduler api --host 0.0.0.0 --port 8000
    start_service worker env SERVICE_NAME=scheduler-worker async-scheduler worker --workers 2 --max-concurrent 16
    start_service scheduler env SERVICE_NAME=scheduler-cron async-scheduler scheduler-service --poll-interval 15
    start_service reconciler env SERVICE_NAME=scheduler-reconciler async-scheduler reconciler-service --interval 20
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
    echo "Usage: $0 {start|stop|restart|status}"
    exit 1
    ;;
esac
