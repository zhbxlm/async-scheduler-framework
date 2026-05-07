# P1 Architecture Evolution Design

Date: 2026-05-07
Status: Draft
Depends on:
- `docs/plans/2026-05-07-p0-architecture-remediation-design.md`
- `docs/plans/2026-05-07-p0-architecture-remediation-plan.md`

---

## 1. Background

P0 completed the foundational cleanup:
- MySQL moved toward single durable source of truth
- Redis task state became a rebuildable cache/index layer
- consumer-side global concurrency semantics were reduced
- control-plane loops were extracted from API processes

P1 should not repeat P0. Instead, it should convert the platform from a working split architecture into a more explicit, auditable, and evolvable system.

The next weaknesses are now clearer:
1. task lifecycle rules are still implicit and distributed across components
2. callback delivery is still process-driven instead of transactionally staged
3. API routes still own too much orchestration logic
4. definitions and execution instances are not fully separated
5. lease / repair semantics are improved but not yet fully formalized

---

## 2. P1 Goals

### Goal A — Explicit lifecycle state machine
Introduce a first-class task state machine shared by creation, execution, completion, cancellation, and repair.

### Goal B — Durable callback outbox
Move callback delivery from opportunistic direct send + retry into a durable outbox model.

### Goal C — Application service layer
Separate HTTP route concerns from domain orchestration logic.

### Goal D — Definition / instance separation
Separate task/DAG/schedule definitions from concrete execution instances.

### Goal E — Formal lease and repair semantics
Clarify which runtime structures express:
- execution ownership
- scheduler index state
- repair eligibility
- stale recovery boundaries

---

## 3. Non-goals

The following are not part of this P1 wave:
- full product UX redesign
- multi-region scheduling
- external workflow engine replacement
- event sourcing migration of the entire task domain
- full tenancy/billing redesign

---

## 4. Current Problems

## 4.1 Lifecycle rules are implicit
Transitions currently happen in multiple places:
- task creation path
- task executor
- task completion node
- task reconciler
- cancellation endpoints

The system works, but the transition rules are not centralized.

### Effects
- difficult to prove legal transitions
- different components may apply subtly different rules
- recovery logic can diverge from live execution logic

## 4.2 Callback delivery is not yet transactionally staged
Final state persistence and callback dispatch are adjacent, but callback dispatch is still not modeled as a durable, queryable delivery pipeline.

### Effects
- callback reliability depends on process behavior more than durable workflow state
- it is harder to inspect, replay, or audit callback delivery
- repair logic and dispatch logic remain too intertwined

## 4.3 API routes still own orchestration decisions
Task routes currently perform more than transport mapping.

### Effects
- business rules are harder to reuse outside HTTP
- CLI / SDK / background tasks cannot cleanly share a use-case layer
- testability is lower than it could be

## 4.4 Definition and execution instance concepts are mixed
The system already has DAGs, schedules, and tasks, but it still lacks a clean execution-instance layer.

### Effects
- harder to support run history, rerun, replay, step retry, or audit
- schedule-triggered instances and ad hoc task instances are not equally modeled

## 4.5 Lease and repair semantics are still partially implicit
P0 removed one confusing concurrency mechanism, but the remaining model still needs explicit documentation and code-level ownership boundaries.

### Effects
- stale recovery rules are harder to reason about
- running index and execution ownership still need a cleaner formal contract

---

## 5. Target Architecture (P1)

## 5.1 Lifecycle state machine

A central `TaskStateMachine` becomes the only authority for legal task transitions.

### Proposed statuses
- `pending`
- `scheduled`
- `queued`
- `running`
- `completed`
- `failed`
- `cancelled`

### Proposed transition examples
- `pending -> scheduled`
- `pending -> queued`
- `scheduled -> queued`
- `queued -> running`
- `running -> completed`
- `running -> failed`
- `queued -> cancelled`
- `running -> failed` (stale lease recovery)
- `running -> queued` (controlled requeue path only)

### Design rule
All state transitions must pass through a single state transition function or service.

This state machine should encode:
- current status
- triggering event
- target status
- allowed/forbidden condition
- side effects to schedule (not directly perform)

---

## 5.2 Callback outbox model

Instead of immediately coupling finalization and callback delivery, task completion should stage callback work durably.

### Proposed durable entities
- `TaskRecord` — final lifecycle truth
- `CallbackOutboxRecord` — callback delivery intent and status

### Completion flow
1. task result is persisted
2. final task status is persisted
3. callback outbox row is created in the same durable transaction
4. separate callback dispatcher reads outbox rows and sends callbacks
5. delivery result updates outbox state
6. retries operate on outbox state, not only transient Redis state

### Benefits
- inspectable callback backlog
- replayable delivery
- easier dead-letter behavior
- cleaner separation of completion vs transport

---

## 5.3 Application service layer

Introduce service/use-case classes that own orchestration logic and allow route handlers to remain thin.

### Candidate services
- `TaskSubmissionService`
- `TaskQueryService`
- `TaskCancellationService`
- `TaskResultService`
- `ScheduleManagementService`
- `CallbackDispatchService`

### API route responsibility after refactor
Routes should only do:
- request parsing
- auth / tenant context mapping
- response translation
- HTTP-specific status code selection

Routes should not directly encode workflow transitions or durable orchestration rules.

---

## 5.4 Definition / instance separation

Introduce an explicit distinction between:
- **definitions**: reusable declarative intent
- **instances/runs**: one concrete execution attempt

### Proposed concepts
- `DAGDefinition`
- `ScheduleDefinition`
- `TaskTemplate` (optional if needed)
- `DagRun`
- `TaskRun` or `TaskInstance`

### Minimal P1 requirement
Even if the full model is not implemented yet, P1 should create the shape required to distinguish:
- “what should run”
- “what actually ran this time”

---

## 5.5 Formal runtime ownership semantics

Define separate meanings for:

### Durable state
Owned by MySQL task records and outbox records.

### Scheduler execution index
Owned by Redis queue/running index.

### Execution ownership
Owned by task lease / lock.

### Repair trigger
Derived from durable state + stale lease + missing runtime artifacts.

### Design rule
No single runtime artifact should silently mean two things.

---

## 6. Recommended design choices

## 6.1 State machine implementation style

### Option A — scattered helper functions
Pros:
- low diff

Cons:
- preserves implicit architecture

### Option B — dedicated state machine module
Pros:
- explicit
- testable
- reusable by reconciler and live execution path

### Decision
Choose **Option B**.

---

## 6.2 Callback delivery model

### Option A — keep inline callback + Redis retry
Pros:
- minimal implementation effort

Cons:
- weaker auditability
- more process-coupled

### Option B — outbox + dispatcher
Pros:
- durable
- observable
- replayable

### Decision
Choose **Option B**.

---

## 6.3 Service layer introduction style

### Option A — full rewrite of routes
Pros:
- cleanest end state

Cons:
- too much churn for P1

### Option B — gradual extraction
Pros:
- safer migration
- easier review

### Decision
Choose **Option B**.

---

## 7. Data model evolution (P1)

## 7.1 TaskRecord
P1 may need small extensions, but TaskRecord remains the lifecycle anchor.

Potential additions:
- explicit lease-related timestamps if not already derivable
- richer failure classification fields
- execution attempt lineage fields if needed

## 7.2 CallbackOutboxRecord
Proposed fields:
- id
- task_id
- tenant_id
- callback_url
- payload_json
- delivery_status (`pending`, `delivered`, `failed`, `dead_letter`)
- attempt_count
- next_attempt_at
- last_error
- created_at
- updated_at

## 7.3 Optional run entities
If introduced in P1 scope:
- `DagRun`
- `TaskRun` / `TaskInstance`

If not fully implemented, at minimum design should reserve the concept.

---

## 8. Acceptance Criteria

P1 is considered complete when:

### Lifecycle
- all state transitions are validated through one explicit state machine module
- reconciler and live execution use the same transition rules

### Callback delivery
- callback dispatch is driven from durable outbox state
- callback retries are observable and replayable

### Application layer
- primary task routes delegate orchestration to service/use-case classes
- route handlers become transport-oriented

### Modeling
- definitions and execution instances are no longer conceptually conflated
- at least one execution-instance concept exists or is formally staged for introduction

### Runtime semantics
- running index, lease ownership, and repair triggers are explicitly documented and separately enforced

---

## 9. Risks

### Risk 1 — state machine migration churn
Centralizing transitions may touch many files.

Mitigation:
- start with transition wrapper + adapter pattern
- move components one by one

### Risk 2 — callback outbox migration complexity
Completion logic will become more transactional.

Mitigation:
- introduce outbox in parallel with compatibility path
- migrate delivery path in controlled stages

### Risk 3 — service layer can over-abstract
Too much layering too quickly can slow delivery.

Mitigation:
- extract only high-value use cases first
- keep services small and concrete

---

## 10. Recommended next artifact

After approval, create:
- `docs/plans/2026-05-07-p1-architecture-evolution-plan.md`

That implementation plan should likely sequence work as:
1. state machine
2. callback outbox
3. service layer extraction
4. definition / instance refactor staging
5. lease / repair contract cleanup
