# async-scheduler-framework

> Ray-powered asynchronous task scheduling framework with multi-tenant support, DAG orchestration, split task/ops APIs, and production-grade observability.

[![CI](https://github.com/zhbxlm/async-scheduler-framework/actions/workflows/ci.yml/badge.svg)](https://github.com/zhbxlm/async-scheduler-framework/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org)
[![Tests](https://img.shields.io/badge/tests-138%20passed-brightgreen)](#testing)

---

## Documentation

| Document | Description |
|----------|-------------|
| [docs/API.md](docs/API.md) | REST API reference |
| [docs/STRUCTURE.md](docs/STRUCTURE.md) | Project structure overview |
| [docs/configuration.md](docs/configuration.md) | Runtime configuration reference |
| [docs/deployment/docker.md](docs/deployment/docker.md) | Docker deployment notes |

---

## Architecture

```text
                     +----------------------+
                     |      clients         |
                     +----------+-----------+
                                |
               +----------------+----------------+
               |                                 |
     +---------v---------+             +---------v---------+
     |   task-api:8001   |             |    ops-api:8000   |
     | /tasks /health/*  |             | /ops/v1/* /health |
     +---------+---------+             +---------+---------+
               |                                 |
               +----------------+----------------+
                                |
                      +---------v---------+
                      |   Redis registry  |
                      |   + queue state   |
                      +---------+---------+
                                |
                  +-------------+--------------+
                  |                            |
         +--------v--------+          +--------v--------+
         | MySQL TaskRecord|          | CronScheduler   |
         | task persistence|          | ops-api bg task |
         +--------+--------+          +-----------------+
                  |
         +--------v--------+
         | TaskReconciler  |
         | task-api bg task|
         +--------+--------+
                  |
         +--------v--------+
         |  DagEngine /    |
         |  QueueManager   |
         +--------+--------+
                  |
         +--------v--------+
         | Workers / Proxy |
         | / Node Agent    |
         +-----------------+
```

### Entry points

| Entry point | File | Purpose |
|-------------|------|---------|
| Ops API | `src/main.py` | Cluster/node/capability/DAG/schedule/ops management |
| Task API | `src/main_tasks.py` | Task create/list/get/result/cancel |
| Node Agent | `src/agent/server.py` | Per-node heartbeat, ownership, cluster invite/release |

---

## Quick Start

For a **5‑minute quickstart** with split APIs (ops‑api + task‑api), see **[docs/getting‑started/quickstart.md](docs/getting‑started/quickstart.md)**.

### Docker Compose

```bash
# Start full stack
docker compose up
```

More commonly:

```bash
# Start both API services + infra
docker compose up scheduler-ops-api scheduler-task-api redis mysql

# Start observability stack too
docker compose --profile observability up

# Run tests
docker compose --profile test run --rm test
```

### Local development (quick reference)

```bash
# Install runtime + dev dependencies
pip install -e .[dev]

# Run ops API (Redis only)
export REDIS_URL=redis://localhost:6379/0 DEPLOYMENT_ROLE=ops-api SERVICE_NAME=scheduler-ops-api
uvicorn src.main:app --reload --port 8000

# Run task API (Redis + MySQL)
export REDIS_URL=redis://localhost:6379/0 MYSQL_URL=mysql://root:secret@localhost:3306/async_scheduler DEPLOYMENT_ROLE=task-api SERVICE_NAME=scheduler-task-api
uvicorn src.main_tasks:app --reload --port 8001
```

### API URLs

- Ops API: `http://localhost:8000`
- Task API: `http://localhost:8001`
- Ops Swagger: `http://localhost:8000/docs`
- Task Swagger: `http://localhost:8001/docs`

---

## Runtime roles

### ops-api

Responsibilities:
- capability / cluster / node / DAG / schedule / tenant management
- queue and system ops views
- cron background scheduling
- health and observability endpoints

Background task:
- `CronScheduler`

Depends on:
- Redis
- optional tracing backend

### task-api

Responsibilities:
- task submission
- task query / result retrieval / cancellation
- task consistency repair

Background task:
- `TaskReconciler`

Depends on:
- Redis
- MySQL (`TaskRecord` persistence)

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `MYSQL_URL` | _(empty)_ | MySQL URL; required for `task-api` task persistence |
| `ENVIRONMENT` | `development` | `development` / `testing` / `production` |
| `BACKGROUND__RECONCILE__ENABLED` | `false` | Enable task reconciler loop |
| `BACKGROUND__CRON__ENABLED` | `false` | Enable cron scheduler loop |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(empty)_ | Jaeger / Tempo OTLP endpoint |

Recommended split deployment:
- `ops-api`: `BACKGROUND__CRON__ENABLED=true`, `BACKGROUND__RECONCILE__ENABLED=false`
- `task-api`: `BACKGROUND__CRON__ENABLED=false`, `BACKGROUND__RECONCILE__ENABLED=true`

---

## Testing

```bash
# All tests
python -m pytest tests/ -v

# With coverage
python -m pytest tests/ --cov=src --cov-report=term-missing
```

Current status:
- 138 tests passed

---

## API overview

### Ops API (`:8000`)

| Prefix | Description |
|--------|-------------|
| `/ops/v1/capabilities` | Register and manage capabilities |
| `/ops/v1/clusters` | Cluster registry and management |
| `/ops/v1/dags` | DAG definition management |
| `/ops/v1/nodes` | Node registry and state management |
| `/ops/v1/schedules` | Cron schedule management |
| `/ops/v1/tenants` | Tenant management |
| `/ops/v1` | Queue / stats / ops endpoints |
| `/health/*` | Health / readiness / metrics |

### Task API (`:8001`)

| Prefix | Description |
|--------|-------------|
| `/tasks` | Create, list, get, get result, cancel |
| `/health/*` | Health / readiness / metrics |

---

## User-facing packages

The repo also contains independently packaged user-side components:

| Package dir | Package name | Purpose |
|-------------|--------------|---------|
| `packages/node-agent` | `async-agent` | Lightweight node agent |
| `packages/async-proxy` | `async-proxy` | Sidecar for long-running service async wrapping |
| `packages/worker-sdk` | `async-worker` | Worker SDK / base classes |

---

## Observability

Health endpoints are available on both APIs:

```bash
GET /health/
GET /health/ready
GET /health/detailed
GET /health/metrics
```

Tracing:
- set `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317`

---

## Notes

- `src/main.py` is now the **ops-api** entrypoint.
- `src/main_tasks.py` is now the **task-api** entrypoint.
- `src/main_task_api.py` has been removed.
- Old `amu_*` package naming has been renamed to `async_*` in packaged components.
