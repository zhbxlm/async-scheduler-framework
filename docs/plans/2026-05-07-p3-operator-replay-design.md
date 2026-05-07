# P3 Operator & Replay Productization Design

Date: 2026-05-07
Status: Draft
Depends on:
- `docs/plans/2026-05-07-p2-platform-hardening-design.md`

---

## 1. Background

P0/P1/P2 introduced the essential platform primitives:
- control-plane split
- durable lifecycle and callback structures
- execution-instance models
- timeline/audit foundations
- ops-facing callback and timeline query endpoints

P3 should turn these primitives into a coherent operator-facing recovery and replay system.

The goal is not just to store more records, but to make platform failures and interventions manageable through explicit models and safe operations.

---

## 2. P3 Goals

### Goal A — Operator intervention model
Represent manual operator actions explicitly:
- replay task
- replay callback
- cancel queued/running work
- mark dead-letter acknowledged
- force stale recovery

### Goal B — Run-centric replay model
Replays should target runs and attempts rather than mutating task records blindly.

### Goal C — Productized dead-letter / retry ops
Operators should have a clear control surface for inspecting and replaying failures.

### Goal D — Full eventized timeline
All meaningful task/run/operator actions should produce timeline events.

### Goal E — Lease / recovery controls
Operators should be able to understand and safely act on stale ownership / stuck tasks.

---

## 3. Non-goals

- full UI implementation
- external approval workflow system
- cross-region disaster recovery productization
- billing / quota product packaging

---

## 4. Design Directions

## 4.1 Operator action model

Introduce an explicit intervention record, for example:
- `OperatorActionRecord`

### Example actions
- `task_replay_requested`
- `callback_replay_requested`
- `task_cancel_requested`
- `dead_letter_acknowledged`
- `stale_recovery_forced`

Each action should carry:
- actor
- target entity
- reason
- payload
- created_at

This creates auditability for human interventions.

---

## 4.2 Replay model

Replay should not directly mutate “truth” without lineage.

### Principles
- replay creates a new `TaskRun` or run attempt lineage
- original run remains immutable history
- replay reason is recorded
- timeline reflects original failure and replay request separately

---

## 4.3 Dead-letter operations surface

P2 added dead-letter listing and replay foundations.
P3 should make this operationally complete with:
- filters
- acknowledgement state
- replay history
- replay safety checks
- operator action audit

---

## 4.4 Eventized timeline completeness

P3 should ensure timeline captures:
- task lifecycle transitions
- task run lifecycle
- callback delivery lifecycle
- repair actions
- operator actions
- replay lineage

---

## 4.5 Lease / recovery controls

P3 should productize understanding of:
- which lease is active
- why a task is considered stale
- what recovery action is legal
- when a replay is safer than a repair

---

## 5. Acceptance Criteria

P3 is complete when:
- operator interventions are durable and auditable
- replay creates lineage instead of overwriting history
- dead-letter handling is productized enough for operations usage
- timeline includes system + operator actions
- stale/lease recovery is queryable and explainable

---

## 6. Recommended next artifact

After approval:
- `docs/plans/2026-05-07-p3-operator-replay-plan.md`
