# P1 Architecture Evolution Implementation Plan

Date: 2026-05-07
Status: Draft
Depends on:
- `docs/plans/2026-05-07-p1-architecture-evolution-design.md`

---

## Execution order

Recommended P1 sequence:

1. explicit lifecycle state machine
2. callback outbox
3. application service extraction
4. definition / instance separation staging
5. lease / repair semantics cleanup

This order minimizes churn because it stabilizes transition rules before changing dispatch and route structure.

---

# Phase 1 — Explicit lifecycle state machine

## Task 1.1 — Inventory existing transition points

### Goal
Find every place that mutates task status directly.

### Likely files
- `src/platform/task_creator.py`
- `src/platform/task_executor.py`
- `src/platform/task_completion_node.py`
- `src/platform/task_reconciler.py`
- `src/api/routes/tasks.py`

### Deliverable
A transition map covering:
- current status
- event
- next status
- owner component

---

## Task 1.2 — Introduce `TaskStateMachine` module

### Goal
Create a dedicated module that validates allowed transitions.

### Requirements
- central transition table
- transition validation API
- test coverage for legal and illegal transitions

### Target file
- `src/platform/task_state_machine.py`

---

## Task 1.3 — Migrate completion path to state machine

### Goal
Task completion must use centralized transition rules.

### Files
- `src/platform/task_completion_node.py`
- `src/platform/task_executor.py`

---

## Task 1.4 — Migrate reconciler repair path to state machine

### Goal
Repair logic should reuse the same legal transition rules as live execution.

### Files
- `src/platform/task_reconciler.py`
- `src/platform/task_state_machine.py`

---

## Task 1.5 — Migrate API cancellation and task creation paths

### Goal
API-triggered lifecycle transitions should use the same state machine.

### Files
- `src/api/routes/tasks.py`
- `src/platform/task_creator.py`

---

# Phase 2 — Callback outbox

## Task 2.1 — Add durable callback outbox model

### Goal
Introduce `CallbackOutboxRecord`.

### Files
- new `src/models/callback_outbox.py`
- migrations

---

## Task 2.2 — Stage callback intent in completion transaction

### Goal
Completion writes callback outbox rows durably instead of relying on immediate dispatch.

### Files
- `src/platform/task_completion_node.py`

---

## Task 2.3 — Add callback dispatcher service

### Goal
Background dispatcher scans durable outbox and delivers callbacks.

### Files
- new `src/services/callback_dispatcher.py`
- control-plane wiring

---

## Task 2.4 — Add retry / dead-letter semantics on outbox

### Goal
Retry policy should operate on durable outbox state.

### Files
- `src/services/callback_dispatcher.py`
- outbox model
- monitoring/metrics

---

## Task 2.5 — Migrate observability to outbox metrics

### Goal
Expose pending / failed / delivered callback states via ops/metrics.

---

# Phase 3 — Application service layer

## Task 3.1 — Extract `TaskSubmissionService`
## Task 3.2 — Extract `TaskQueryService`
## Task 3.3 — Extract `TaskCancellationService`
## Task 3.4 — Extract `TaskResultService`
## Task 3.5 — Update routes to become transport-only

### Likely files
- `src/services/*.py`
- `src/api/routes/tasks.py`

---

# Phase 4 — Definition / instance separation

## Task 4.1 — Introduce conceptual run model

### Goal
Define `DagRun` / `TaskRun` or equivalent instance representation.

## Task 4.2 — Link schedule-triggered executions to run instances

## Task 4.3 — Reserve room for rerun / replay / audit flows

---

# Phase 5 — Lease / repair contract cleanup

## Task 5.1 — Document runtime ownership boundaries in code
## Task 5.2 — Align repair triggers with durable state + lease state
## Task 5.3 — Add regression tests for stale ownership recovery

---

# Final verification

## V1 — end-to-end lifecycle tests
- create
- queue
- run
- complete
- cancel
- repair

## V2 — callback outbox tests
- durable enqueue
- dispatcher send
- retry
- dead-letter

## V3 — route/service boundary tests
- routes do not own orchestration logic

---

## Recommended execution mode

Suggested grouping for implementation:
- Group A: 1.1–1.5 (state machine)
- Group B: 2.1–2.5 (callback outbox)
- Group C: 3.1–3.5 (service layer)
- Group D: 4.1–5.3 + verification
