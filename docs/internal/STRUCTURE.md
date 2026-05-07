# Project Structure

> Updated on 2026-05-05 after API split and package renaming.

```text
async-scheduler-framework/
├── config/
│   ├── dags/
│   ├── service_defaults.yaml
│   ├── settings_compat.py
│   └── settings_pydantic.py
├── docs/
│   ├── deployment/
│   ├── plans/
│   ├── reference/
│   ├── runtime/
│   ├── API.md
│   ├── STRUCTURE.md
│   ├── configuration.md
│   └── testing.md
├── examples/
├── packages/
│   ├── async-proxy/
│   │   ├── async_proxy/
│   │   ├── pyproject.toml
│   │   └── README.md
│   ├── node-agent/
│   │   ├── async_agent/
│   │   ├── pyproject.toml
│   │   └── README.md
│   └── worker-sdk/
│       ├── async_worker/
│       ├── pyproject.toml
│       └── README.md
├── scripts/
├── src/
│   ├── agent/
│   │   ├── config.py
│   │   ├── ray_manager.py
│   │   ├── resource_detector.py
│   │   └── server.py
│   ├── api/
│   │   ├── routes/
│   │   │   ├── capabilities.py
│   │   │   ├── clusters.py
│   │   │   ├── dags.py
│   │   │   ├── health.py
│   │   │   ├── nodes.py
│   │   │   ├── ops.py
│   │   │   ├── schedules.py
│   │   │   ├── tasks.py
│   │   │   └── tenants.py
│   │   ├── auth.py
│   │   ├── dependencies.py
│   │   ├── middleware.py
│   │   └── schemas.py
│   ├── cli/
│   ├── common/
│   │   ├── async_db.py
│   │   ├── container.py
│   │   ├── error_handling.py
│   │   ├── http_client.py
│   │   ├── lifecycle.py
│   │   ├── logging_config.py
│   │   ├── redis_client.py
│   │   └── tracing.py
│   ├── models/
│   │   ├── capability.py
│   │   ├── cluster.py
│   │   ├── dag.py
│   │   ├── deploy.py
│   │   ├── node.py
│   │   ├── schedule.py
│   │   ├── task.py
│   │   ├── tenant.py
│   │   └── tenant_context.py
│   ├── platform/
│   │   ├── base_registry.py
│   │   ├── capability_registry.py
│   │   ├── circuit_breaker.py
│   │   ├── cluster_registry.py
│   │   ├── cron_scheduler.py
│   │   ├── dag_engine.py
│   │   ├── dag_loader.py
│   │   ├── node_registry.py
│   │   ├── queue_keys.py
│   │   ├── queue_manager.py
│   │   ├── remote_code_fetcher.py
│   │   ├── schedule_registry.py
│   │   ├── step_executors.py
│   │   ├── task_completion_node.py
│   │   ├── task_consumer.py
│   │   ├── task_creator.py
│   │   ├── task_executor.py
│   │   ├── task_reconciler.py
│   │   └── tenant_registry.py
│   ├── proxy/
│   │   ├── async_command_proxy.py
│   │   └── async_service_proxy.py
│   ├── sdk/
│   │   └── client.py
│   ├── workload/
│   │   ├── actor_pool_manager.py
│   │   ├── async_proxy_worker.py
│   │   ├── base_worker_actor.py
│   │   ├── scheduler_actor.py
│   │   └── worker_dev_kit.py
│   ├── main.py
│   └── main_tasks.py
├── tests/
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── requirements.txt
├── README.md
└── setup.py
```

## Entry points

### `src/main.py`
Ops API entrypoint.

Responsibilities:
- capability / cluster / node / DAG / schedule / tenant management
- ops and queue inspection endpoints
- cron background scheduling
- health endpoints

### `src/main_tasks.py`
Task API entrypoint.

Responsibilities:
- create/list/get/cancel tasks
- task result retrieval
- task persistence and reconciliation
- health endpoints

### `src/agent/server.py`
Node Agent entrypoint.

Responsibilities:
- node ownership
- heartbeat
- invite/release Ray cluster membership
- package deployment to node

## Service container split

`src/common/container.py` exposes three factories:
- `build()` — all-in-one container
- `build_ops_api()` — ops-api subset
- `build_task_api()` — task-api subset

## User-facing packaged components

### `packages/node-agent/async_agent/`
Standalone Python package `async-agent`.

### `packages/async-proxy/async_proxy/`
Standalone Python package `async-proxy`.

### `packages/worker-sdk/async_worker/`
Standalone Python package `async-worker`.

## Removed legacy items

The following historical items are no longer part of the active structure:
- `src/main_task_api.py`
- `src/common/db.py`
- `src/common/events.py`
- `src/common/protocols.py`
- `src/platform/registry_interface.py`
- `src/platform/task_router.py`
- `src/platform/raydata_client.py`
- `src/platform/quota_enforcer.py`
- `src/platform/resource_manager.py`
