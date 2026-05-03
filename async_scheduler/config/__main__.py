"""CLI for inspecting current configuration.

Run with:
    python -m async_scheduler.config
    python -m async_scheduler.config --json
"""

from __future__ import annotations

import json
import sys
from typing import Any

from async_scheduler.config import ConfigContext, config


def main() -> None:
    args = sys.argv[1:]
    json_output = "--json" in args
    env_output = "--env" in args

    if env_output:
        # Show environment variables affecting config
        relevant = [
            "ENVIRONMENT",
            "DATABASE_URL",
            "REDIS_URL",
            "QUEUE_TYPE",
            "LOG_LEVEL",
            "LOG_FORMAT",
            "SERVICE_NAME",
            "NODE_ID",
        ]
        data = {key: os.environ.get(key, "") for key in relevant}
        print(json.dumps(data, indent=2))
        return

    with ConfigContext():
        # Ensure no leftovers from previous runs
        pass

    data = {
        "environment": config.environment.value,
        "is_production": config.is_production,
        "is_test": config.is_test,
        "is_local": config.is_local,
        "database": {
            "url": config.database.url,
            "is_sqlite": config.database.is_sqlite,
            "is_mysql": config.database.is_mysql,
            "echo": config.database.echo,
            "pool_size": config.database.pool_size,
        },
        "logging": {
            "level": config.logging.level,
            "format": config.logging.fmt,
            "service_name": config.logging.service_name,
            "node_id": config.logging.node_id,
        },
        "backends": {
            "queue_type": config.backends.queue_type,
            "lock_type": config.backends.lock_type,
            "registry_type": config.backends.registry_type,
            "redis_url": config.backends.redis_url,
            "lease_ttl_seconds": config.backends.lease_ttl_seconds,
            "heartbeat_interval_seconds": config.backends.heartbeat_interval_seconds,
        },
    }

    if json_output:
        print(json.dumps(data, indent=2))
    else:
        from pprint import pprint

        pprint(data)


if __name__ == "__main__":
    main()