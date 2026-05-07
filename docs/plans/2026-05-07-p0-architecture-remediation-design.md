# P0 Architecture Remediation Design

Date: 2026-05-07
Status: Draft
Scope: P0 only

---

## 1. Background

The current async-scheduler-framework architecture has already evolved from a monolithic task service into a split model with:

- **Task API**: task submission/query/cancellation/result
- **Ops API**: operations/registry/management
- **Redis**: queueing, locks, registries, retry state
- **MySQL**: durable task persistence
- **Background loops**: cron, reconciliation, compensation

This is a meaningful improvement, but several P0 issues remain:

1. **Source of truth is still blurred** between MySQL and multiple Redis representations
2. **Concurrency ownership is duplicated** across queue, consumer, global slot, running set, and lock
3. **Control-plane logic still lives inside API processes**, making deployment semantics unclear

This document proposes a P0 remediation design that fixes those three issues in the smallest order-preserving sequence.

---

## 2. P0 Goals

### Goal A — Single source of truth
Make **MySQL** the only durable source of truth for task lifecycle state.

Redis must only hold:
- scheduling indexes
- locks / leases
- ephemeral cache
- retry queues that can be rebuilt or replayed from durable state

### Goal B — Single concurrency model
Reduce runtime concurrency semantics to a single layered model:
- scheduling concurrency
- worker-local execution concurrency
- optional distributed execution lease

Each layer must have a distinct responsibility and must not duplicate another layer’s meaning.

### Goal C — Clear control-plane separation
Remove long-running control-plane responsibilities from API processes.

Target runtime roles:
- task-api
- ops-api
- control-plane worker

---

## 3. Non-goals

The following are explicitly not part of this P0 wave:

- full DAG entity redesign
- callback outbox refactor
- task event timeline system
- deep application service refactor
- complete API surface redesign

These are important but belong to P1/P2.

---

## 4. Current Problems

### 4.1 Blurred source of truth
Task state exists in multiple places:
- MySQL `TaskRecord`
- Redis `task:*`
- Redis pending/running queue structures
- Redis task locks
- Redis callback retry queue

This creates two architectural problems:
1. repair logic must reason across several partial truths
2. Redis state expiry can remove information that is still needed for repair or audit

### 4.2 Duplicated concurrency semantics
Concurrency is currently expressed by several overlapping mechanisms:
- QueueManager max concurrent
- TaskConsumer local semaphore
- `_GLOBAL_CONC_KEY`
- running zset
- task execution lock

These mechanisms are not purely layered; some of them describe the same concept from different places.

### 4.3 Control loops inside API services
Long-running loops currently depend on API process lifecycle:
- TaskReconciler
- CompensationService
- CronScheduler

This creates ambiguity in scaling semantics and causes API deployment topology to affect background execution topology.

---

## 5. Target Architecture

## 5.1 Runtime topology

```text
             +----------------+
             |    Ops API     |
             | config/manage  |
             +----------------+
                      |
                      v
+------------+   +----------------+   +----------------------+
|  Clients    |-->|   Task API     |-->|       MySQL          |
| SDK / CLI   |   | submit/query   |   | source of truth      |
+------------+   +----------------+   +----------------------+
                      |
                      v
                +------------+
                |   Redis    |
                | queue/index|
                +------------+
                      ^
                      |
             +----------------------+
             |  Control-plane worker |
             | cron/reconcile/repair |
             +----------------------+
                      ^
                      |
             +----------------------+
             | Worker / Agent nodes  |
             +----------------------+
```

---

## 5.2 Data ownership model

### MySQL owns
Durable task facts:
- task identity
- task status
- retry metadata
- scheduling intent
- final result / error
- callback intent metadata (later expandable to outbox)

### Redis owns
Ephemeral execution support:
- pending queue
- running queue / running index
- capability queue stats cache
- distributed leases / locks
- short-lived caches

### Key rule
If Redis is fully lost, the system must still be able to reconstruct executable state from MySQL + definitions.

That means Redis cannot be the only place where a repair-critical fact exists.

---

## 5.3 Concurrency model

### Layer 1 — Scheduling concurrency
Owned by scheduler/queue logic.

Purpose:
- control how many tasks of a capability or tenant are allowed into active execution

Representation:
- capability-level queue policy
- tenant quota policy

### Layer 2 — Worker-local execution concurrency
Owned by TaskConsumer / worker runtime.

Purpose:
- prevent a process from overloading itself

Representation:
- local semaphore only

### Layer 3 — Distributed execution lease
Owned by a single lease mechanism.

Purpose:
- prevent duplicate execution across nodes
- express current execution ownership

Representation:
- one lease/lock per running task

### Explicit removals
The system must not have multiple independent definitions of “running”.

Therefore:
- running zset = scheduler execution index only
- distributed lock = execution ownership only
- local semaphore = process-local capacity only

No extra global slot abstraction unless it is the single distributed lease model.

---

## 5.4 Control-plane split

### task-api
Responsibilities:
- create task
- query task
- cancel task
- fetch result
- lightweight read-through cache access if needed

Must not host:
- cron loop
- reconciliation loop
- compensation loop

### ops-api
Responsibilities:
- capability registry
- cluster registry
- node registry
- schedule config management
- tenant config management
- DAG definition management
- operational visibility endpoints

Must not host:
- task creation core path
- long-running repair loops

### control-plane worker
Responsibilities:
- CronScheduler execution loop
- TaskReconciler loop
- CompensationService loop
- future callback dispatcher / repair workers

This worker is the only process type that owns platform background control loops.

---

## 6. Ordered Remediation Plan (P0 sequence)

## Step 1 — Single source of truth first
This is first because concurrency and repair semantics depend on it.

### Step 1.1
Reclassify Redis `task:*` as cache only.

### Step 1.2
Ensure every repairable task fact exists in MySQL.

Minimum required durable fields:
- task_id
- tenant_id
- task_type / capability
- scheduling intent
- status
- retry metadata
- output / error
- timestamps needed for repair

### Step 1.3
Refactor TaskReconciler to repair from MySQL outward, not Redis inward.

Priority order:
1. read durable state from MySQL
2. reconstruct or validate queue/index state in Redis
3. repair missing Redis artifacts

### Step 1.4
Document Redis loss recovery rules.

Expected property:
- Redis flush should degrade service temporarily but not lose durable task truth

---

## Step 2 — Concurrency simplification
This is second because once the source of truth is stable, runtime semantics can be cleaned without mixing data repair and scheduling concerns.

### Step 2.1
Audit all “running” indicators and assign each one a single meaning.

### Step 2.2
Pick exactly one distributed ownership primitive.

Recommended:
- keep task execution lease / lock
- demote/remove `_GLOBAL_CONC_KEY` unless it becomes the single lease abstraction

### Step 2.3
Keep local semaphore as process protection only.

### Step 2.4
Keep queue max concurrent as scheduling policy, not execution ownership.

---

## Step 3 — Control-plane extraction
This is third because the runtime loop split is easier once truth and concurrency semantics are stable.

### Step 3.1
Add a dedicated control-plane startup entrypoint.

Example role set:
- `src/main_control.py`
- container builder for control plane

### Step 3.2
Move CronScheduler/TaskReconciler/CompensationService startup there.

### Step 3.3
Remove these loops from task-api lifespan.

### Step 3.4
Keep ops-api and task-api stateless with respect to long-running orchestration.

---

## 7. Design Trade-offs

## Option 1 — Keep everything in task-api
Pros:
- smallest implementation diff
- easiest deployment today

Cons:
- scaling semantics remain blurry
- task traffic and control loops still coupled
- future platform separation becomes harder

### Decision
Rejected for P0 final state.

## Option 2 — Split control-plane into separate process
Pros:
- clean runtime topology
- easier to scale independently
- easier to reason about ownership

Cons:
- requires one more startup target and deployment unit

### Decision
Chosen.

---

## 8. Acceptance Criteria

P0 is considered complete when all of the following are true:

### Source of truth
- MySQL is the only durable lifecycle source
- Redis task cache loss does not lose repair-critical task truth
- Reconciler repairs Redis from MySQL, not the reverse

### Concurrency
- there is one explicit distributed execution ownership model
- local and distributed concurrency semantics are documented and non-overlapping
- duplicate “running” meanings are removed

### Control-plane split
- task-api and ops-api do not host long-running control loops
- control-plane worker can be started independently
- deployment docs reflect the new topology

---

## 9. Risks

### Risk 1 — Partial migration period
During refactor, old and new semantics may coexist briefly.

Mitigation:
- gate each step behind tests
- keep compatibility shims temporarily
- move one ownership boundary at a time

### Risk 2 — Redis rebuild edge cases
Rebuilding queue/index state from MySQL can accidentally duplicate scheduled entries if dedup is weak.

Mitigation:
- explicit dedup keys
- deterministic queue reconstruction rules
- replay-safe enqueue semantics

### Risk 3 — Operational migration complexity
Adding a control-plane process changes deployment.

Mitigation:
- keep task-api-compatible transitional mode during rollout
- add docs + compose examples before removal

---

## 10. Recommended next document

After this design is approved, the next artifact should be an implementation plan split into small tasks:

1. P0-1 single source of truth implementation plan
2. P0-2 concurrency simplification plan
3. P0-3 control-plane extraction plan

Each task should be sized for TDD-first implementation and isolated review.
