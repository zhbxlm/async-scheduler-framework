# Capability & Install Matrix

## Overview

This document defines what is included in each installable package, what optional extras exist, and which capabilities each deployment mode provides.

---

## Package Layout

| Package | PyPI Name | Purpose |
|---|---|---|
| Root | `async-scheduler-framework` | Full monolith install (control-plane + task-api + ops-api) |
| `packages/task-api` | `async-scheduler-task-api` | Task submission API service only |
| `packages/ops-api` | `async-scheduler-ops-api` | Ops/admin API service only |
| `packages/control-plane` | `async-scheduler-control-plane` | Standalone control-plane worker |
| `packages/worker` | `async-scheduler-worker` | Ray-based worker node |
| `packages/agent` | `async-scheduler-agent` | Node agent (lightweight deploy) |
| `packages/proxy` | `async-scheduler-proxy` | Fire-and-forget dispatch proxy (embed into existing services) |
| `packages/sdk` | `async-scheduler-sdk` | Client SDK for task submission |
| `packages/cli` | `async-scheduler-cli` | `scheduler` CLI tool |

---

## Core Dependencies (always required)

| Dependency | Reason |
|---|---|
| `redis>=5.0` | Queue, distributed state, pub/sub |
| `sqlalchemy>=2.0` | ORM for task records |
| `pydantic>=2.0` | Data validation and settings |
| `pydantic-settings>=2.0` | Config loading |
| `orjson>=3.9` | Fast JSON serialization |

---

## Optional Extras

### `[tracing]`

Enables distributed tracing via OpenTelemetry.

```
pip install async-scheduler-framework[tracing]
```

**Included packages:**
- `opentelemetry-api`
- `opentelemetry-sdk`
- `opentelemetry-semantic-conventions`
- `opentelemetry-exporter-otlp`

**Runtime behavior without this extra:**
- Tracing instrumentation is silently skipped.
- No errors or warnings at startup.
- All other functionality unaffected.

**Available for:**
- Root package
- `async-scheduler-task-api`
- `async-scheduler-ops-api`

---

### `[dev]`

Development and testing tooling.

```
pip install async-scheduler-framework[dev]
```

**Included packages:**
- `pytest`, `pytest-asyncio`, `pytest-cov`
- `fakeredis`
- `aiosqlite` (in-memory async DB for integration tests)
- `httpx` (async HTTP client for tests)
- `ruff` (linter)

---

## Per-Package Capability Matrix

| Capability | root | task-api | ops-api | control-plane | worker | agent | proxy | sdk | cli |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Task submission API | ✅ | ✅ | — | — | — | — | ✅ | ✅ | ✅ |
| Task query / cancel | ✅ | ✅ | — | — | — | — | — | ✅ | ✅ |
| Ops/admin API | ✅ | — | ✅ | — | — | — | — | — | — |
| Cron scheduling | ✅ | — | — | ✅ | — | — | — | — | — |
| Task reconciler | ✅ | — | — | ✅ | — | — | — | — | — |
| Callback dispatch | ✅ | — | — | ✅ | — | — | — | — | — |
| Worker execution | ✅ | — | — | — | ✅ | — | — | — | — |
| Node agent | ✅ | — | — | — | — | ✅ | — | — | — |
| Fire-and-forget proxy | ✅ | — | — | — | — | — | ✅ | — | — |
| Tracing (optional) | ✅ | ✅ | ✅ | — | — | — | — | — | — |
| DB migrations (Alembic) | ✅ | — | ✅ | — | — | — | — | — | — |

---

## Deployment Profiles

### Minimal (task submission only)
Install: `async-scheduler-task-api`

Provides: task submit, query, cancel endpoints.
Does not include: control-plane background loops, ops dashboard.

---

### Full service (monolith)
Install: `async-scheduler-framework`

Provides: all capabilities in a single process via `main.py`.
Suitable for: development, small deployments.

---

### Microservice split (recommended for production)
Install separately:
- `async-scheduler-task-api` — handles client-facing task requests
- `async-scheduler-ops-api` — handles admin/ops requests
- `async-scheduler-control-plane` — runs cron / reconcile / compensation / callback dispatch
- `async-scheduler-worker` — runs task execution via Ray
- `async-scheduler-agent` — manages worker node lifecycle

Control-plane can run as a dedicated standalone process/package.

---

### Embedded dispatch (existing services)
Install: `async-scheduler-proxy`

Embed into any existing FastAPI/Flask/WSGI service.
Provides: fire-and-forget async task dispatch without running a separate service.

---

## Notes on Optional Capability Policy

1. **Core capabilities** are in every install of a package by default.
2. **Optional extras** (`[tracing]`) are explicitly declared and gracefully degraded when not installed.
3. **Runtime integration add-ons** (e.g., custom Ray executors, external alert webhooks) are documented separately and require no package changes.
4. Subpackages that carry no tracing code do not declare a tracing extra (worker, agent, proxy, sdk, cli).
