#!/usr/bin/env sh
set -eu

mkdir -p /app/data

if [ "${INIT_DB_ON_START:-false}" = "true" ]; then
  async-scheduler init-db-cmd || true
fi

exec "$@"
