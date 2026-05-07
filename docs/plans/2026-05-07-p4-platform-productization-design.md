# P4 Platform Productization Design

Date: 2026-05-07
Status: Draft
Depends on:
- `docs/plans/2026-05-07-p3-operator-replay-design.md`

---

## 1. Background

P0–P3 established the architectural and operational foundations:
- split control-plane
- durable callback and lifecycle models
- execution-instance lineage
- audit timelines
- operator action and replay foundations

P4 should focus on productization. The objective is no longer just internal correctness, but creating a platform that is understandable, controllable, and safe for operators and organizational stakeholders.

---

## 2. P4 Goals

### Goal A — Operator experience
Turn existing operator primitives into a coherent operational surface with filtering, summaries, and actionable dashboards.

### Goal B — Recovery explainability and controls
Operators should understand why a task is stale, what recovery options are available, and what the risks are.

### Goal C — Replay safety policies
Replay should become policy-driven, with safety checks and boundaries rather than unconditional mutation.

### Goal D — Run-centric workflow productization
TaskRun / DagRun should become the default operator mental model for execution history and intervention.

### Goal E — Governance and risk controls
Introduce the first explicit policy boundary for who can do what, when, and under what audit constraints.

---

## 3. Non-goals

- full external IAM integration
- final UI implementation
- complete approvals workflow productization
- billing / quota commercialization

---

## 4. Design Directions

## 4.1 Operator surface

P4 should group existing capabilities into coherent operational views:
- callback health
- dead-letter backlog
- replay queue
- stale task queue
- recent operator actions
- recent task timelines

The system already has most primitives. P4 should shape them into a productized control surface.

---

## 4.2 Recovery explainability

Operators should be able to inspect:
- why a task is considered stale
- whether lease exists or expired
- whether replay is safer than repair
- what prior repair attempts occurred
- what prior operator interventions occurred

This requires explanation-oriented queries, not just raw state dumps.

---

## 4.3 Replay safety policy engine

Replay should become policy-aware.

### Example rules
- deny replay for tasks currently holding a valid lease
- require reason for replay
- limit replay frequency per task/run
- distinguish callback replay from task replay
- require elevated permission for force replay on stale/ambiguous ownership

P4 does not need a full policy DSL, but it should define structured policy checks.

---

## 4.4 Run-centric product model

Operators should interact with runs, not only tasks.

### Desired views
- task with all runs
- dag with all runs
- run lineage tree
- replay lineage chain
- worker/lease association history

This moves the platform away from flat status inspection toward lineage-aware operational reasoning.

---

## 4.5 Governance / risk control boundary

P4 should define early governance primitives such as:
- intervention role categories
- action reason requirements
- audit completeness expectations
- force-operation risk markers
- protected operations requiring stronger authorization

This does not need enterprise IAM yet, but it must define the contract.

---

## 5. Acceptance Criteria

P4 is complete when:
- operator actions are queryable and meaningfully filterable
- stale recovery becomes explainable rather than opaque
- replay operations have explicit safety checks
- run lineage becomes the default operational view
- governance boundaries for high-risk interventions are defined and partially enforced

---

## 6. Recommended next artifact

After approval:
- `docs/plans/2026-05-07-p4-platform-productization-plan.md`
