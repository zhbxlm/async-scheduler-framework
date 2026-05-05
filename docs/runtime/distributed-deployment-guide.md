# Distributed Deployment Guide

> Historical/runtime reference. Parts of this document describe an earlier distributed-kernel direction and are **not fully aligned** with the current `ops-api` / `task-api` split. For the current deployment shape, prefer:
> - `README.md`
> - `docs/deployment/docker.md`
> - `docs/configuration.md`

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
# Historical example from an earlier design phase.
# The current repository no longer uses `ray_async.backends.BackendConfig`
# as the primary runtime entry described here.
from ray_async.backends import BackendConfig

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

## 10. Concrete startup examples

### Local split script with config validation

```bash
DATABASE_URL=mysql+asyncmy://user:pass@127.0.0.1:3306/ray_async \
REDIS_URL=redis://127.0.0.1:6379/0 \
bash scripts/run_local_split.sh validate

DATABASE_URL=mysql+asyncmy://user:pass@127.0.0.1:3306/ray_async \
REDIS_URL=redis://127.0.0.1:6379/0 \
bash scripts/run_local_split.sh dry-run

DATABASE_URL=mysql+asyncmy://user:pass@127.0.0.1:3306/ray_async \
REDIS_URL=redis://127.0.0.1:6379/0 \
bash scripts/run_local_split.sh start
```

The script now injects `DEPLOYMENT_ROLE`, `SERVICE_NAME`, and stable local `NODE_ID`s
for each service, and validates runtime settings before startup.


### Example A — single host, multiple processes

Recommended when:
- validating true distributed mode on one machine
- separating API and worker/reconciler responsibilities during development

Suggested process split:
- process 1: API
- process 2: worker / consumer
- process 3: reconciler

Example commands (shape only; adapt to your wrapper/config style):

```bash
# process 1
# Historical CLI example (outdated for current split APIs)
ray-async api --init-db

# process 2
# Historical CLI example (outdated for current repository shape)
ray-async worker --workers 2 --max-concurrent 10

# process 3
ray-async reconcile
```

Operational checklist:
- all processes point at the same Redis URL
- all worker/consumer processes use distributed mode config
- reconciler runs with the same lease / liveness expectations as workers
- observability is checked via `/debug/summary` and `/debug/leases/anomalies`
- verify worker registration via `/workers`

### Example B — small multi-node topology

Recommended when:
- validating lease ownership and recovery across machines
- testing dead-owner and stale-lease behavior in a more realistic shape

Suggested topology:
- node A: API + observability access
- node B: worker / consumer process
- node C: worker / consumer process + reconciler
- shared Redis: external or managed service reachable by all nodes

Example rollout order:
1. start Redis / confirm connectivity from all nodes
2. start API on node A
3. start one worker on node B
4. start one worker plus reconciler on node C
5. submit a task and verify worker registration / lease visibility

Operational checklist:
- keep clocks reasonably synchronized across nodes
- use the same Redis database / namespace intentionally
- start with conservative heartbeat / TTL values rather than tiny test values
- verify worker visibility via `/workers` and `/workers/{worker_id}/leases`
- verify anomaly summary remains empty under healthy steady-state conditions

## 11. Suggested tuning profiles

### Development distributed profile
- `lease_ttl_seconds = 30`
- `heartbeat_interval_seconds = 10`
- reconciler interval: moderate / not overly aggressive
- best for: first real multi-process bring-up

### Faster recovery profile
- `lease_ttl_seconds = 15`
- `heartbeat_interval_seconds = 5`
- only use after verifying Redis latency and event-loop stability are acceptable
- best for: recovery-focused staging validation

### Higher-latency / safer profile
- `lease_ttl_seconds = 45`
- `heartbeat_interval_seconds = 15`
- best for: less predictable environments or early conservative rollout

### Avoid in real runtime
- sub-second TTLs used in fault-injection tests
- extremely aggressive reconciler cadence without validating false-positive repair risk
- mixing test-style TTLs with cross-node deployments

## 12. Recommended next runtime hardening work

- document Redis connection pooling expectations
- document worker/reconciler deployment patterns in even more detail
- add stress scenarios for long-running workloads
- add anomaly-oriented observability for operators
- add deployment manifests / scripts when runtime shape stabilizes further
