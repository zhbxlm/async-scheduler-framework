# Redis Distributed Kernel Implementation Plan

Date: 2026-05-01
Project: ray-async-framework
Branch: feat/deepwiki-distributed-alignment
Related design: docs/plans/2026-05-01-redis-distributed-kernel-design.md
Status: Approved for implementation

## Execution Mode

Subagent-driven development with mandatory TDD.

Each task must follow:
1. write/extend failing tests
2. implement the smallest viable change
3. run targeted tests
4. run broader regression where needed
5. commit only when green

## Success Criteria

The milestone is complete only when all of the following are true:

- Redis backend can run queue + lease semantics across multiple workers
- task claim ownership is externally visible and lease-based
- worker heartbeat and stale-lease recovery works
- completion handling is idempotent under duplicate delivery
- distributed reconciler can safely repair stale tasks
- multi-worker integration tests pass
- smoke/documentation updated for distributed mode

---

## Task 1 — Add Redis dependencies and backend configuration scaffolding

### Goal
Introduce the minimal dependency/config plumbing required to run the framework in Redis-backed distributed mode.

### Files
- pyproject.toml
- ray_async/backends/factory.py
- ray_async/backends/__init__.py
- ray_async/platform/services.py
- README.md
- tests/* (new/updated as needed)

### Steps
1. Add Redis client dependency and optional test dependency support.
2. Extend backend config to accept Redis URL and lease/heartbeat settings.
3. Ensure service container can be built in distributed mode without breaking memory mode.
4. Document configuration knobs briefly.

### Tests first
- config parsing/build test for Redis mode
- service container build test for Redis config path (can use mocked client initially)

### Verify
- `pytest -q` subset for config/service tests passes

---

## Task 2 — Implement RedisQueueBackend

### Goal
Provide a real Redis-backed queue implementation supporting immediate enqueue/dequeue, delayed tasks, requeue, cancellation, and stats.

### Files
- ray_async/backends/redis.py (new)
- ray_async/backends/factory.py
- ray_async/queue/manager.py
- tests/test_redis_queue_backend.py (new)
- tests/test_queue_manager_distributed.py (new or updated)

### Scope
- ready queues separated by priority
- delayed tasks stored in sorted set by ready_at
- promotion path from delayed -> ready
- dequeue returns candidate task for claim phase
- cancellation/requeue support
- queue size/stat inspection

### Tests first
- enqueue/dequeue immediate task
- delayed task not visible before ready time
- delayed task promoted when due
- requeue preserves semantics
- cancel removes queued/delayed task
- queue stats reflect actual state

### Verify
- targeted Redis queue tests pass

---

## Task 3 — Implement Redis lease/lock backend

### Goal
Introduce token-safe lease semantics for distributed task ownership.

### Files
- ray_async/backends/redis.py
- tests/test_redis_lease_backend.py (new)
- possibly ray_async/backends/base.py if abstractions need extension

### Scope
- acquire(key, ttl) -> tokenized handle
- release(handle) only if token matches
- extend(handle, ttl) only if token matches
- is_locked(key)
- safe expiry behavior

### Tests first
- acquire succeeds when unlocked
- second acquire fails while lease active
- release with wrong token fails
- extend with wrong token fails
- expired lease can be reacquired

### Verify
- targeted lease tests pass

---

## Task 4 — Add worker identity and heartbeat subsystem

### Goal
Make workers externally visible and track liveness in distributed mode.

### Files
- ray_async/worker/base.py
- ray_async/platform/services.py
- ray_async/api/app.py
- ray_async/distributed/worker_registry.py (new)
- tests/test_worker_registry.py (new)

### Scope
- worker ID generation/configuration
- heartbeat registration/update in Redis
- liveness query
- clean shutdown deregistration where reasonable
- optional health endpoint exposure

### Tests first
- worker registers and heartbeats
- stale worker becomes not-live after TTL
- multiple workers appear independently

### Verify
- worker registry tests pass

---

## Task 5 — Introduce durable execution-attempt modeling

### Goal
Persist distributed execution attempts so ownership and recovery are inspectable and repairable.

### Files
- ray_async/persistence/models.py
- ray_async/persistence/repositories.py
- ray_async/core/models.py
- migration/init logic as needed
- tests/test_execution_attempts.py (new)

### Scope
- execution attempt model/table
- create/update/finalize attempt operations
- worker_id / status / started_at / completed_at / retry_index / lease_token fields
- task -> latest attempt linkage where useful

### Tests first
- create attempt
- update heartbeat/started state
- finalize success/failure
- query latest attempt per task

### Verify
- execution attempt tests pass

---

## Task 6 — Wire claim + lease flow into execution path

### Goal
Ensure only a valid lease-holder may execute a task and that running tasks renew ownership while alive.

### Files
- ray_async/core/consumer.py
- ray_async/executor/executor.py
- ray_async/queue/manager.py
- ray_async/platform/services.py
- tests/test_distributed_claim_flow.py (new)

### Scope
- dequeue candidate task
- acquire task lease before execute
- create/update attempt record on claim/start
- run heartbeat/lease extension loop during execution
- release/finalize correctly on terminal state
- on lease failure, abandon or requeue safely

### Tests first
- two workers race and only one executes
- heartbeat extends active lease
- lease loss during execution causes controlled failure/recovery behavior

### Verify
- distributed claim tests pass

---

## Task 7 — Add completion idempotency layer

### Goal
Make duplicate completion delivery converge safely.

### Files
- ray_async/platform/completion.py
- ray_async/persistence/repositories.py
- ray_async/backends/redis.py
- tests/test_completion_idempotency.py (new)

### Scope
- define completion key strategy
- Redis fast dedupe + durable confirmation path if needed
- repeated terminal completion must not double-apply side effects
- callback/handler invocation guarded against duplicates where possible

### Tests first
- duplicate success completion only processes once
- duplicate failure completion only processes once
- repeated callback delivery remains convergent

### Verify
- completion idempotency tests pass

---

## Task 8 — Upgrade reconciler to distributed repair v2

### Goal
Repair stale tasks in a shared-state environment without conflicting with live workers.

### Files
- ray_async/platform/reconciler.py
- ray_async/distributed/repair.py (new, optional)
- ray_async/api/app.py
- tests/test_distributed_reconciler.py (new)

### Scope
- detect expired lease on claimed/running tasks
- detect dead workers via heartbeat view
- requeue retryable stale tasks
- mark exhausted tasks failed
- use reconciler lock to avoid competing repairers
- preserve idempotency under repeated runs

### Tests first
- stale leased task gets requeued
- dead worker task gets recovered
- active leased task is not stolen
- two reconcilers do not double-repair the same task

### Verify
- distributed reconciler tests pass

---

## Task 9 — Add multi-worker integration and failure-injection tests

### Goal
Prove the system behaves as distributed, not just that components compile.

### Files
- tests/integration/test_multi_worker_redis.py (new)
- tests/integration/test_failure_recovery.py (new)
- scripts/smoke_test.py
- CI/test docs as needed

### Scope
- multi-worker task processing with shared Redis
- worker crash/restart simulation
- lease expiry recovery
- duplicate completion convergence
- delayed task promotion under distributed mode

### Tests first
- integration scenarios themselves define the spec

### Verify
- integration Redis suite passes locally

---

## Task 10 — Docs, smoke path, and distributed-mode polish

### Goal
Make the new distributed mode usable by humans instead of only passing tests.

### Files
- README.md
- examples/end_to_end_demo.md
- scripts/smoke_test.py
- ray_async/api/app.py
- config examples if added

### Scope
- document Redis startup/config
- distributed mode run instructions
- debugging hints for worker/lease/reconciler status
- extend smoke test or add distributed smoke variant

### Tests first
- smoke or doc-verified command path if practical

### Verify
- README instructions are consistent with code
- smoke path passes

---

## Suggested Execution Order

1. Task 1
2. Task 2
3. Task 3
4. Task 4
5. Task 5
6. Task 6
7. Task 7
8. Task 8
9. Task 9
10. Task 10

## Review Gates

After each implementation task:
- implementation review against the design
- code-quality review for unnecessary complexity / regressions

## Final Verification

Minimum final verification command set (expected to evolve):

- `pytest -q`
- targeted Redis/distributed integration suite
- `python3 scripts/smoke_test.py`
- distributed smoke path if added

## Notes

- Prefer clear semantics over clever Redis tricks.
- Keep memory mode working.
- Do not claim exactly-once.
- Any ambiguity in transition ordering should be resolved by explicit invariants in code/tests.
