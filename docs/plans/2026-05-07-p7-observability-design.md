# P7 Observability Productization Design

Date: 2026-05-07
Status: Draft
Depends on:
- `docs/plans/2026-05-07-p6-tech-debt-hardening-design.md`

---

## 1. Background

P0–P6 established a complete platform skeleton with durable audit, replay, operator experience, and governance. However, the system currently lacks production-grade observability:

- No Prometheus metrics for platform-level events (replay count, dead-letter count, operator actions)
- No structured log standard across service layer
- Health check endpoint only covers basic liveness

P7 addresses this by productizing observability across three dimensions:
1. Prometheus metrics for operator/replay/dead-letter events
2. Structured logging standard for service layer
3. Extended health check surface

---

## 2. P7 Goals

### Goal A — Platform metrics
Expose Prometheus counters/gauges for:
- `replay_requests_total` (by task_id label, actor_role)
- `dead_letter_callbacks_total` (by status: acked, replayed, pending)
- `operator_actions_total` (by action_type)
- `force_operations_total` (by operation, actor_role)
- `stale_tasks_gauge` (current count of stale/running-without-lease tasks)

### Goal B — Structured logging
- All service-layer writes emit structured log entries (JSON-compatible)
- Log fields: timestamp, service, operation, task_id, actor, outcome
- Use existing `src/common/logging_config.py` as foundation

### Goal C — Extended health check
- `/health/ready` — deep readiness: DB reachable, Redis reachable
- `/health/platform` — platform-level summary: dead-letter backlog size, stale task count, recent operator action count

---

## 3. Non-goals

- External APM integration (Datadog, New Relic)
- Distributed tracing productization (OpenTelemetry)
- Alerting rule changes

---

## 4. Design Details

### 4.1 Platform metrics

Extend `src/common/metrics.py` with new counters:

```python
replay_requests_total = Counter("replay_requests_total", "Task replay requests", ["actor_role", "allowed"])
dead_letter_callbacks_total = Counter("dead_letter_callbacks_total", "Dead-letter callback events", ["event"])
operator_actions_total = Counter("operator_actions_total", "Operator actions recorded", ["action_type"])
force_operations_total = Counter("force_operations_total", "Force operations executed", ["operation", "outcome"])
stale_tasks_gauge = Gauge("stale_tasks_gauge", "Current count of stale task runs")
```

Emit at call sites:
- `ReplayPolicyService.check_task_replay_allowed` → `replay_requests_total`
- `CallbackOpsService.replay_dead_letter` / `acknowledge_dead_letter` → `dead_letter_callbacks_total`
- `OperatorActionService.record` → `operator_actions_total`
- `ForceOperationService.execute_force_operation` → `force_operations_total`

### 4.2 Structured logging

Add `src/common/service_logger.py`:

```python
def log_service_event(service, operation, *, task_id=None, actor=None, outcome="ok", **extra):
    logger.info(json.dumps({
        "ts": datetime.utcnow().isoformat(),
        "service": service,
        "operation": operation,
        "task_id": task_id,
        "actor": actor,
        "outcome": outcome,
        **extra,
    }))
```

Call at key write paths in service layer.

### 4.3 Extended health check

Add to `src/api/routes/health.py`:
- `GET /health/ready` — checks DB and Redis connectivity
- `GET /health/platform` — queries dead-letter backlog count, stale task count, recent operator action count

---

## 5. Acceptance Criteria

P7 is complete when:
- 5 new Prometheus metrics are emitted at correct call sites
- Structured log events emitted for replay, dead-letter, operator actions, force ops
- `/health/ready` returns DB + Redis reachability
- `/health/platform` returns platform-level summary
- All 527+ tests still pass
