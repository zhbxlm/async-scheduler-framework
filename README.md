# async-scheduler-framework

> Ray-powered asynchronous task scheduling framework with multi-tenant support, DAG orchestration, and production-grade observability.

[![CI](https://github.com/zhbxlm/async-scheduler-framework/actions/workflows/ci.yml/badge.svg)](https://github.com/zhbxlm/async-scheduler-framework/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org)
[![Tests](https://img.shields.io/badge/tests-163%20passed-brightgreen)](#testing)

---

## 📖 Documentation

| Document | Description |
|----------|-------------|
| [docs/API.md](docs/API.md) | Full REST API reference (58 endpoints, auto-generated) |
| [docs/STRUCTURE.md](docs/STRUCTURE.md) | Project structure overview (auto-generated) |
| [docs/openapi.json](docs/openapi.json) | OpenAPI 3.x JSON schema |
| [docs/deepwiki-reference/](docs/deepwiki-reference/) | Architecture design reference |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────┐
│                  API Layer (FastAPI)                 │
│  /tasks  /capabilities  /clusters  /nodes  /health  │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│               Platform Services                      │
│  QueueManager  CronScheduler  TaskReconciler        │
│  CapabilityRegistry  ClusterRegistry  NodeRegistry  │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│              Infrastructure                          │
│         Redis (queue/registry)  MySQL (tasks)        │
└─────────────────────────────────────────────────────┘
```

**Three entry points:**

| Entry point | File | Purpose |
|-------------|------|---------|
| Main API | `src/main.py` | Scheduling, registry, DAG, ops |
| Task API | `src/main_task_api.py` | Lightweight task CRUD only |
| Node Agent | `src/agent/server.py` | Per-node heartbeat & ownership |

---

## 🚀 Quick Start

### Docker Compose (recommended)

```bash
# Start full stack
docker compose up

# Start with observability tools (Jaeger, Prometheus, Grafana)
docker compose --profile observability up

# Run test suite
docker compose --profile test run --rm test
```

### Local development

```bash
# Install dependencies
pip install -r requirements.txt

# Configure (copy and edit)
cp .env.example .env

# Run main API
uvicorn src.main:app --reload --port 8000

# Run task API (separate process)
uvicorn src.main_task_api:app --reload --port 8001
```

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `MYSQL_URL` | _(empty)_ | MySQL URL; if empty, uses in-memory only |
| `ENVIRONMENT` | `development` | `development` / `testing` / `production` |
| `BACKGROUND__RECONCILE__ENABLED` | `false` | Enable task reconciler |
| `BACKGROUND__CRON__ENABLED` | `false` | Enable cron scheduler |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(empty)_ | Jaeger/Tempo OTLP endpoint |

---

## 🧪 Testing

```bash
# All tests
python -m pytest tests/ -v

# With coverage
python -m pytest tests/ --cov=src --cov-report=term-missing

# Performance benchmarks
python -m pytest tests/test_benchmarks.py -v -s

# Standalone benchmark report
python tests/test_benchmarks.py
```

**Test suite:** 163 tests — unit, integration, health checks, benchmarks.

### Benchmark results (mocked Redis, 0.1ms simulated latency)

| Path | mean | p99 | TPS |
|------|------|-----|-----|
| BaseRedisRegistry.get | 1.2ms | 1.2ms | 850/s |
| CapabilityRegistry.get | 0.02ms | 0.04ms | 46K/s |
| TaskCreator.create_task | 0.04ms | 0.1ms | 25K/s |
| JSON round-trip (TaskRecord) | 0.03ms | 0.04ms | 36K/s |
| @log_errors overhead | ~0ms | ~0ms | 2.3M/s |

---

## 📡 API Overview

Base URL: `http://localhost:8000`

| Tag | Endpoints | Description |
|-----|-----------|-------------|
| `tasks` | 5 | Create, list, get, result, cancel tasks |
| `capabilities` | 3 | Register and manage capabilities |
| `clusters` | 4 | Cluster registry and resource management |
| `nodes` | 4 | Node registration and state management |
| `schedules` | 3 | Cron schedule management |
| `tenants` | 3 | Tenant management and API keys |
| `dags` | 4 | DAG definition management |
| `ops` | 8 | Operations: queues, health, stats |
| `health` | 6 | Health checks and Prometheus metrics |

Full reference: [docs/API.md](docs/API.md) or `GET /docs` (Swagger UI).

---

## 🔍 Observability

### Health checks

```bash
GET /health/        # Basic health
GET /health/ready   # Readiness probe (K8s)
GET /health/detailed # System + process metrics
GET /health/metrics  # Prometheus text format
```

### Distributed tracing

Set `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317` to export traces.
View at `http://localhost:16686` (Jaeger UI, via docker compose profile).

### Prometheus + Grafana

```bash
docker compose --profile observability up prometheus grafana
# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000  (admin/admin)
```

---

## 📦 Key Components

| Module | Purpose |
|--------|---------|
| `src/common/async_db.py` | Async SQLAlchemy engine & session |
| `src/common/error_handling.py` | Structured errors + `@log_errors` decorator |
| `src/common/lifecycle.py` | Background task lifecycle manager |
| `src/common/tracing.py` | OpenTelemetry tracing setup |
| `src/platform/base_registry.py` | Generic Redis CRUD base class |
| `src/platform/queue_manager.py` | Priority queue with Lua atomic enqueue |
| `src/platform/task_creator.py` | Task creation adapter (Redis + DB) |
| `src/platform/task_reconciler.py` | Stuck-task detection with leader election |
| `src/platform/cron_scheduler.py` | Cron-based task scheduling |
| `config/settings_pydantic.py` | Type-safe config with pydantic-settings |

---

## 📄 Generating docs

```bash
# Regenerate all docs from live app
python scripts/generate_docs.py --all

# Individual targets
python scripts/generate_docs.py --schema     # openapi.json
python scripts/generate_docs.py --api        # API.md
python scripts/generate_docs.py --structure  # STRUCTURE.md
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Run tests: `python -m pytest tests/`
4. Push and open a PR — CI will run lint, tests, security scan, and Docker build automatically.
