# P0 Architecture Remediation Implementation Plan

Date: 2026-05-07
Status: Draft
Depends on: `docs/plans/2026-05-07-p0-architecture-remediation-design.md`

---

## Execution strategy

This plan follows the approved P0 order:

1. **Single source of truth**
2. **Concurrency model simplification**
3. **Control-plane extraction**

Implementation rule:
- each task must be small and reviewable
- tests come first where possible
- no cross-cutting refactor without explicit acceptance criteria
- compatibility shims are allowed temporarily if they reduce migration risk

---

# Phase 1 — Single source of truth

## Task 1.1 — Document and codify Redis task cache as non-authoritative

### Goal
Make `Redis task:*` explicitly cache-only in code and docs.

### Changes
- update docstrings/comments in `TaskCreator`, `TaskReconciler`, `TaskCompletionNode`
- rename internal terminology where needed from “task record” to “task cache” when referring to Redis
- add tests or assertions preventing repair logic from requiring Redis task cache as durable truth

### Files
- `src/platform/task_creator.py`
- `src/platform/task_reconciler.py`
- `src/platform/task_completion_node.py`
- `docs/architecture/*.md`

### Verify
- code no longer describes Redis task payload as authoritative lifecycle state
- tests/documentation reflect MySQL-first truth model

---

## Task 1.2 — Audit durable task fields and fill gaps in MySQL model

### Goal
Ensure MySQL contains every repair-critical task fact.

### Checklist
Durable fields must cover:
- task_id
- tenant_id
- task_type/capability
- status
- callback_url (or callback intent)
- retry metadata required by repair logic
- scheduling intent (`scheduled_at`, `delay_seconds`, `cron_expr` if applicable)
- timestamps needed for timeout/recovery decisions
- failure/error payload required for debugging

### Changes
- inspect `TaskRecord`
- add missing columns if required
- update model serialization/deserialization paths
- add migration if schema changed

### Files
- `src/models/task.py`
- migration files / alembic
- `src/platform/task_creator.py`
- `src/platform/task_completion_node.py`

### Verify
- new task creation persists all repair-critical fields to MySQL
- tests confirm fields survive Redis cache loss

---

## Task 1.3 — Add regression tests for Redis cache loss recovery

### Goal
Prove the system can retain task truth when Redis task cache is missing.

### Test scenarios
1. task exists in MySQL, Redis `task:*` missing
2. terminal task exists in MySQL, callback retry state missing
3. pending/running index missing, durable task still present

### Changes
- write tests before reconciler refactor
- model expected rebuild behavior clearly

### Files
- `tests/...` new reconciliation tests
- `src/platform/task_reconciler.py`

### Verify
- tests fail before reconciler changes if behavior is not implemented
- tests pass after Task 1.4/1.5

---

## Task 1.4 — Refactor TaskReconciler to read durable truth from MySQL first

### Goal
Make reconciliation direction explicit: **MySQL -> Redis**, not **Redis -> MySQL** as the primary path.

### Changes
- add durable scan/query path from MySQL
- identify tasks that require Redis repair
- reconstruct missing queue/index/cache artifacts from MySQL state
- keep Redis scan only as auxiliary cleanup path if still needed

### Constraints
- do not delete all Redis scan logic at once if still needed for transitional cleanup
- prioritize deterministic rebuild semantics

### Files
- `src/platform/task_reconciler.py`
- `src/common/transaction.py`
- `src/services/compensation.py`

### Verify
- reconciler can repair missing Redis artifacts from MySQL-only durable data
- regression tests from Task 1.3 pass

---

## Task 1.5 — Make task creation and completion replay-safe under repair

### Goal
Ensure queue/index reconstruction will not duplicate execution.

### Changes
- define dedup semantics for rebuild/re-enqueue
- ensure enqueue path is deterministic enough for replay-safe repair
- add idempotent reconstruction guards

### Files
- `src/platform/task_creator.py`
- `src/platform/queue_manager.py`
- `src/platform/task_reconciler.py`

### Verify
- repeated repair pass does not duplicate queue entries
- tests cover repeated reconciliation runs

---

# Phase 2 — Concurrency model simplification

## Task 2.1 — Inventory all runtime concurrency mechanisms in code

### Goal
Produce a code-backed map of current concurrency mechanisms before changing behavior.

### Output
A short markdown section or internal note listing:
- QueueManager limits
- local semaphore
- `_GLOBAL_CONC_KEY`
- running zset
- task lock / lease
- circuit breaker relation to execution gating

### Files
- `src/platform/queue_manager.py`
- `src/platform/task_consumer.py`
- `src/platform/task_executor.py`
- `docs/plans/...plan.md` (update if needed)

### Verify
- each mechanism has exactly one intended meaning documented

---

## Task 2.2 — Choose and enforce a single distributed execution ownership primitive

### Goal
Make one mechanism the canonical distributed execution owner.

### Recommended choice
Use **task execution lease/lock** as canonical distributed ownership.

### Changes
- review whether `_GLOBAL_CONC_KEY` is redundant
- remove or demote redundant global slot logic
- ensure duplicate execution prevention relies on one ownership primitive only

### Files
- `src/platform/task_consumer.py`
- `src/platform/task_executor.py`
- `src/platform/task_reconciler.py`

### Verify
- there is only one distributed execution ownership source
- duplicate execution tests still pass

---

## Task 2.3 — Restrict local semaphore to process-local backpressure only

### Goal
Make worker-local semaphore purely a local safety mechanism.

### Changes
- ensure local semaphore does not encode distributed semantics
- remove implicit coupling between local semaphore and queue correctness

### Files
- `src/platform/task_consumer.py`

### Verify
- local semaphore changes do not affect distributed ownership semantics

---

## Task 2.4 — Re-scope running zset as scheduler execution index only

### Goal
Keep running zset useful, but no longer ambiguous.

### Changes
- document its meaning clearly
- update code paths so running zset is not treated as sole execution truth
- repair logic should reconcile it from durable + lease state if needed

### Files
- `src/platform/queue_manager.py`
- `src/platform/task_reconciler.py`

### Verify
- running zset is no longer overloaded with multiple meanings

---

## Task 2.5 — Add concurrency semantics tests

### Goal
Lock in the new model with explicit tests.

### Test scenarios
- one task cannot be executed by two workers simultaneously
- local semaphore blocks local overload only
- missing running zset does not imply execution loss if lease still exists
- stale lease recovery works without ambiguous ownership

### Files
- `tests/...` new concurrency tests
- `src/platform/task_consumer.py`
- `src/platform/task_reconciler.py`

### Verify
- tests prove semantic separation of queue index / local concurrency / distributed ownership

---

# Phase 3 — Control-plane extraction

## Task 3.1 — Introduce control-plane service container builder

### Goal
Create an explicit container shape for background control loops.

### Changes
- add a control-plane builder in container module
- initialize only the dependencies needed by cron/reconcile/compensation

### Files
- `src/platform/container.py`

### Verify
- control-plane dependencies can start without task-api HTTP app

---

## Task 3.2 — Add `main_control.py` entrypoint

### Goal
Create a standalone process for platform background loops.

### Changes
- create startup entrypoint
- initialize logging/tracing/lifecycle
- register CronScheduler, TaskReconciler, CompensationService
- keep process alive cleanly with graceful shutdown

### Files
- new `src/main_control.py`
- lifecycle helpers if needed

### Verify
- control-plane process starts independently
- loops run without task-api process

---

## Task 3.3 — Remove control loops from task-api lifespan

### Goal
Make task-api stateless with respect to long-running orchestration.

### Changes
- stop registering CronScheduler / TaskReconciler / CompensationService in `main_tasks.py`
- keep only request-serving dependencies

### Files
- `src/main_tasks.py`
- `src/platform/container.py`

### Verify
- task-api can start and serve requests without owning background loops

---

## Task 3.4 — Keep ops-api free of execution loops too

### Goal
Ensure ops-api remains pure management/config service.

### Changes
- confirm no background control loop starts from ops-api
- clean up any lingering lifecycle coupling

### Files
- `src/main.py`
- `src/platform/container.py`

### Verify
- ops-api remains purely management-facing

---

## Task 3.5 — Update deployment/dev docs and compose topology

### Goal
Reflect the new runtime topology in docs and local deployment.

### Changes
- add control-plane worker to docs
- update docker-compose or examples
- clarify startup commands for all roles

### Files
- `docker-compose.yml`
- `docs/deployment/*.md`
- `README.md`

### Verify
- docs show 3-process topology clearly
- local dev startup instructions remain workable

---

# Final verification tasks

## Task V1 — End-to-end P0 scenario tests

### Goal
Verify the architecture works as intended after all P0 changes.

### Scenarios
1. create task -> execute -> complete
2. Redis task cache loss -> reconcile -> state preserved
3. stale lease -> recovery works
4. task-api down/up does not affect control-plane loop ownership
5. control-plane restart recovers loops cleanly

---

## Task V2 — Architecture docs sync

### Goal
Update user-facing and internal architecture docs so they match the new runtime model.

### Files
- `docs/architecture/overview.md`
- `docs/architecture/data-flow.md`
- `docs/guides/operations.md`
- internal docs if still relevant

---

# Recommended execution mode

This plan is suitable for subagent-driven execution in the next phase.

Suggested grouping:
- Group A: Task 1.1–1.5
- Group B: Task 2.1–2.5
- Group C: Task 3.1–3.5
- Group D: V1–V2

Each task should be implemented with:
1. failing test (or explicit reproducible check)
2. minimal code change
3. verification
4. review
