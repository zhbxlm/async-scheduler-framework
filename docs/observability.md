# Observability Strategy

## Profiles

### Minimal (default)
No optional extras required.

- Structured logging via `logging` stdlib (JSON-ready with log adapter)
- Prometheus metrics endpoint (`/metrics`) via `prometheus-client`
- Health check endpoints (`/health`, `/health/live`, `/health/ready`)

Suitable for: development, internal deployments, cost-sensitive environments.

---

### Production (recommended)
Same as minimal, plus:

- Prometheus scrape endpoint exposed and scraped by external collector
- Health checks integrated with orchestrator liveness/readiness probes
- Structured log output forwarded to centralized log aggregator

No extra pip install required.

---

### Tracing-enabled
Install with:

```
pip install async-scheduler-framework[tracing]
# or per service:
pip install async-scheduler-task-api[tracing]
pip install async-scheduler-ops-api[tracing]
```

Provides:
- OpenTelemetry trace context propagation
- OTLP exporter (configurable endpoint via `OTEL_EXPORTER_OTLP_ENDPOINT`)
- Semantic conventions for HTTP and async task spans

Runtime degradation without `[tracing]`:
- Tracing is silently disabled at startup
- No errors or warnings
- All metrics and health checks continue unaffected

---

## Instrumented Components

| Component | Metrics | Logs | Tracing |
|---|:---:|:---:|:---:|
| Task submission API | ✅ | ✅ | ✅ (with extra) |
| Ops/admin API | ✅ | ✅ | ✅ (with extra) |
| Queue manager | ✅ | ✅ | — |
| Callback dispatcher | ✅ | ✅ | — |
| Cron scheduler | ✅ | ✅ | — |
| Task reconciler | ✅ | ✅ | — |
| Worker execution | — | ✅ | — |
| Node agent | — | ✅ | — |

---

## Key Metrics (Prometheus)

Exposed at `/metrics` on task-api and ops-api services.

| Metric | Type | Description |
|---|---|---|
| `scheduler_tasks_submitted_total` | Counter | Tasks submitted |
| `scheduler_tasks_completed_total` | Counter | Tasks completed |
| `scheduler_tasks_failed_total` | Counter | Tasks failed |
| `scheduler_queue_depth` | Gauge | Current pending queue depth per capability |
| `scheduler_running_tasks` | Gauge | Currently running tasks per capability |
| `scheduler_callback_dispatch_total` | Counter | Callback attempts |
| `scheduler_callback_dead_letters_total` | Counter | Callbacks moved to dead letter |

---

## Configuration

Tracing endpoint (when `[tracing]` extra is installed):

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
OTEL_SERVICE_NAME=async-scheduler-task-api
```

Prometheus: no configuration required; `/metrics` is always exposed.

Log level:

```
LOG_LEVEL=INFO  # default
```
