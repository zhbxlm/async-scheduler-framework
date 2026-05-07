# P2 Platform Hardening Design

Date: 2026-05-07
Status: Draft
Depends on:
- `docs/plans/2026-05-07-p0-architecture-remediation-design.md`
- `docs/plans/2026-05-07-p1-architecture-evolution-design.md`

---

## 1. Background

P0 stabilized runtime topology and source-of-truth boundaries.
P1 introduced the first explicit lifecycle, durable callback outbox foundations, service-layer extraction, and a minimal execution-instance model.

P2 should focus on platform hardening rather than architectural rescue.
The system should become:
- easier to observe
- easier to audit
- easier to replay and intervene in
- safer to operate under failure
- more consistent across definitions, runs, retries, and repairs

---

## 2. P2 Goals

### Goal A — Full execution-instance model
Evolve `TaskRun` into a real execution record and introduce `DagRun` where appropriate.

### Goal B — Operational callback maturity
Make callback delivery inspectable, replayable, alertable, and operator-friendly.

### Goal C — Deeper service-layer extraction
Continue shrinking HTTP handlers until orchestration logic consistently lives in application services.

### Goal D — Formal lease / replay / repair model
Strengthen the contract between ownership, stale recovery, requeue, replay, and audit.

### Goal E — Audit and observability
Add task timeline / event history / operational dashboards / intervention workflows.

---

## 3. Non-goals

- full event-sourced rewrite
- multi-region scheduler design
- complete billing/quota productization
- external workflow engine migration

---

## 4. P2 Design Directions

## 4.1 TaskRun / DagRun maturation

P1 added a minimal `TaskRunRecord`. P2 should make it meaningful.

### Target capabilities
- one task can have multiple execution attempts represented as runs
- one DAG invocation can have a `DagRun`
- repair / replay should target runs, not only raw tasks
- operator views should be able to answer:
  - what was scheduled?
  - what actually ran?
  - how many times?
  - on which worker?

### Proposed entities
- `DagRunRecord`
- richer `TaskRunRecord`
- optional step-run record if DAG depth requires it

---

## 4.2 Callback maturity

P1 introduced durable outbox + dispatcher foundations.
P2 should add:
- retry backoff policy visibility
- dead-letter inspection API
- replay / resend operations
- delivery metrics and alert thresholds
- callback timeline events

---

## 4.3 Service-layer completion

P1 extracted a first set of task application services.
P2 should complete the pattern for:
- schedules
n- capabilities
- tenants
- DAG management
- operational interventions

The goal is not abstraction for its own sake, but consistency and testability.

---

## 4.4 Lease / replay / repair formalization

P2 should define:
- lease acquisition
- lease renewal
- lease expiry
- stale detection
- safe replay
- requeue policies
- operator intervention boundaries

This should be documented both in code and docs.

---

## 4.5 Audit / timeline / observability

P2 should add first-class task event history.

### Desired events
- task created
- task queued
- task run created
- lease acquired
- task started
- retry requested
- callback staged
- callback delivered
- callback dead-lettered
- stale recovery applied
- operator replayed / cancelled / retried

These events should support:
- UI/API timeline views
- postmortem analysis
- alert correlation

---

## 5. Acceptance Criteria

P2 is complete when:
- TaskRun is meaningful operationally, not only structurally
- callback delivery can be inspected and replayed safely
- major route families rely on service/application layer orchestration
- lease/replay/repair semantics are explicit and test-backed
- operators can inspect timeline and intervention history

---

## 6. Recommended next artifact

After approval:
- `docs/plans/2026-05-07-p2-platform-hardening-plan.md`
