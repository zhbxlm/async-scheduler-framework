# Distributed Deployment Guide

Date: 2026-05-02
Project: async-scheduler-framework

## Purpose

This guide explains how to run the current repository in a **true distributed mode** rather than the default local/fallback mode.

It focuses on the runtime shape that is already supported today:

- shared Redis-backed queue coordination
- lease-based task ownership
- worker liveness via heartbeat/TTL
- completion dedupe
- reconciler-based stale-task repair

It does **not** claim production-hardening is complete. It is a practical deployment guide for the current repository state.

---

## 1. Runtime modes

### Local / fallback mode
Use this when:
- developing on one machine
- running smoke tests
- Redis is unavailable

Characteristics:
- process-local fallback state is allowed
- good for deterministic local development
- not suitable for true multi-process / multi-node coordination

### Distributed mode
Use this when:
- running multiple consumers/workers
- running separate API / consumer / reconciler processes
- running across multiple machines

Characteristics:
- shared Redis-backed queue / lock / dedupe / worker registry paths are used
- ownership and recovery semantics become externally visible
- this is the mode required for realistic distributed execution

---

## 2. Minimum recommended topology

### Smallest useful distributed topology

- 1 Redis instance (or Redis-compatible service)
- 1 API process
- 1+ consumer/worker process
- 1 reconciler process

### More realistic topology

- 1 Redis instance / managed Redis service
- 2+ consumer/worker processes
- 1 reconciler process
- API process separated from worker processes

### Why separate reconciler
The reconciler is logically different from task execution:
- workers consume and execute tasks
- reconciler repairs stale running tasks and marks abandoned attempts

Running reconciler as its own process makes repair timing and ownership easier to reason about.

---

## 3. Required backend configuration

Use `BackendConfig` with distributed mode enabled.

```python
from async_scheduler.backends import BackendConfig

config = BackendConfig(
    queue_type="redis",
    lock_type="redis",
    registry_type="memory",
    redis_url="redis://localhost:6379/0",
    distributed_mode=True,
    lease_ttl_seconds=30,
    heartbeat_interval_seconds=10,
)
```

### Important notes

- `queue_type="redis"` enables shared queue coordination
- `lock_type="redis"` enables lease-based ownership
- `distributed_mode=True` enables the distributed settings path
- `registry_type` may still remain local depending on current schedule-registry needs

Current critical shared-state paths already using Redis today:
- queue backend
- lock backend
- completion dedupe backend
- worker registry

---

## 4. Lease / heartbeat tuning guidance

### Baseline rule

`heartbeat_interval_seconds` should be comfortably smaller than `lease_ttl_seconds`.

Recommended starting point:
- `lease_ttl_seconds = 30`
- `heartbeat_interval_seconds = 10`

### Safer ratio
A practical starting ratio is:
- heartbeat interval ≈ 1/3 of lease TTL

This gives room for:
- scheduler jitter
- event loop delays
- transient Redis latency spikes

### Avoid too-small TTLs in real deployments
Very small values like `0.05` or `0.1` seconds are useful in tests, but are usually too aggressive for real workloads.

Use tiny TTLs only for:
- fault-injection tests
- recovery race simulations
- local deterministic verification

---

## 5. Process responsibilities

## API process
Recommended responsibilities:
- accept task submissions
- expose observability endpoints
- inspect attempts / leases / repair history

## Worker / consumer process
Recommended responsibilities:
- dequeue tasks
- acquire lease
- create/update execution attempts
- heartbeat lease during execution
- finalize task status through completion flow

## Reconciler process
Recommended responsibilities:
- scan stale running tasks
- detect dead worker + dead lease states
- requeue or mark abandoned according to repair strategy

---

## 6. Observability checklist in distributed mode

When operating in distributed mode, use these endpoints first:

- `/debug/summary`
- `/debug/leases`
- `/debug/leases/{task_id}`
- `/workers`
- `/workers/{worker_id}`
- `/workers/{worker_id}/leases`
- `/reconciler/history`

### Recommended troubleshooting order

1. Check `/debug/summary`
   - look for `running_without_lock_count`
   - look for `locked_but_terminal_count`
   - look for `abandoned_but_running_count`

2. Filter suspicious tasks via `/debug/leases`
   - `task_status=running`
   - `locked_only=true`
   - `worker_id=<id>`

3. Inspect a concrete task via `/debug/leases/{task_id}`

---

## 7. Fallback mode vs distributed mode

### In fallback mode
- process-local state may still be used
- useful for tests and local development
- behavior is deterministic but not truly shared across processes

### In distributed mode
- shared Redis-backed state should be the source of truth for critical paths
- lease and ownership become externally meaningful
- worker liveness and reconciler repair semantics matter operationally

### Practical advice
If you want to validate true distributed behavior, do **not** rely on local smoke tests alone.
Use:
- live Redis verification
- multiple worker processes
- reconciler enabled

---

## 8. Validation before using distributed mode

Recommended targeted validation:

```bash
pytest -q tests/test_observability_api.py
pytest -q tests/integration/test_failure_recovery.py
TEST_REDIS_URL=redis://localhost:6379/0 pytest -q tests/integration/test_live_redis_retry_exhaustion.py
```

Recommended broader live Redis validation:

```bash
TEST_REDIS_URL=redis://localhost:6379/0 python3 scripts/live_redis_suite.py all
```

---

## 9. Current known limitations

The current repository is still not a full production platform.

Known limitations include:
- schedule registry is still primarily local-persistence oriented
- callback / outbound side-effect delivery is still lightweight
- broader stress / scale / long-running churn coverage is still limited
- operational guidance for connection pooling / alerting / dashboards is still minimal

---

## 10. Recommended next runtime hardening work

- add explicit multi-process startup examples
- document Redis connection pooling expectations
- document worker/reconciler deployment patterns in more detail
- add stress scenarios for long-running workloads
- add anomaly-oriented observability for operators
