# Redis Distributed Kernel Design

Date: 2026-05-01
Project: async-scheduler-framework
Branch: feat/deepwiki-distributed-alignment
Status: Draft for approval

## Goal

Evolve the current single-node deepwiki-aligned framework into an aggressive distributed execution kernel using Redis as the first-phase distributed substrate.

The target is not strict exactly-once semantics. The target is a practical, testable distributed scheduler/runtime with:

- multi-worker / multi-instance execution
- shared queue and distributed leases
- worker heartbeat and orphan recovery
- completion idempotency
- reconciler-driven repair on abnormal paths
- end-to-end distributed tests

## Current State

Already present in the repository:

- backend abstraction layer for queue / lock / registry
- in-memory backend implementations
- DAG step executors
- schedule registry lifecycle support
- task completion node
- task reconciler
- capability registry
- local smoke + pytest coverage

Current limitation:

- distributed semantics are architectural only, not operationally real
- state ownership is still effectively local-process oriented
- no real cross-worker claim / lease / recovery path exists

## Non-Goals (Phase 1)

These are intentionally out of scope for the first aggressive distributed implementation:

- strict exactly-once execution
- Kafka / RabbitMQ / multi-broker abstraction parity
- geo-distributed consistency design
- UI / control plane
- advanced autoscaling

## Target Semantics

### Delivery

- task delivery is **at-least-once**
- duplicate execution is possible under failures
- framework correctness depends on idempotent completion handling and repair logic

### Ownership

- a worker must **claim** a task before executing it
- claim ownership is represented by a Redis lease
- active workers must periodically heartbeat / extend lease
- if lease expires, another worker may reclaim the task

### Recovery

- worker crash must not permanently strand tasks
- orphaned claimed/running tasks must be detected and requeued or marked failed according to policy
- reconciler is the slow-path repair mechanism, not the fast-path executor

### Source of Truth

- distributed shared state is the source of truth
- worker local memory is cache/working state only
- task execution transitions must be externally observable and recoverable

## Architecture Decision

## 1. Redis as first-phase distributed substrate

Redis will be used for:

- queue operations
- delayed scheduling handoff if needed
- distributed locks / leases
- worker heartbeats
- ephemeral execution ownership state
- optional idempotency markers and repair coordination

Persistent task and schedule metadata may remain in the existing database layer initially, but must be treated as shared state rather than process-local assumptions.

## 2. Split state into two planes

### Fast plane (Redis)

Used for:
- enqueue / dequeue / claim
- lease ownership
- worker liveness
- retry-ready / scheduled-ready transitions
- dedupe markers with TTL where suitable

### Durable plane (DB existing persistence layer, evolved)

Used for:
- task record
- execution state transitions
- schedule definitions
- execution attempt history
- completion outcomes / callback outcomes where persistence matters

This gives us a realistic hybrid design: Redis for coordination, DB for durable facts.

## 3. Execution model

Proposed execution lifecycle:

1. Task created in durable store
2. Task published to Redis queue
3. Worker dequeues candidate task
4. Worker attempts distributed claim lease for task
5. If claim succeeds:
   - execution attempt record created/updated
   - worker starts heartbeat loop
   - worker executes task / DAG step
6. On success/failure:
   - completion pipeline writes durable outcome
   - completion processing uses idempotency guards
   - lease released or allowed to expire after terminal transition
7. On worker death / lease expiry:
   - reconciler or competing worker detects stale ownership
   - task is requeued or marked failed according to retry policy

## Core Components To Add

## A. RedisQueueBackend

Responsibilities:
- push immediate tasks
- support delayed/scheduled tasks handoff strategy
- pop candidate tasks for workers
- requeue on failure/retry
- expose queue stats

Preferred initial implementation:
- Redis lists or sorted sets for priority/delay
- keep design simple and explicit over clever

Open choice:
- For delayed tasks, use sorted set with score=ready_at timestamp
- For priority, either separate queues per priority or score-based sorted set

Recommendation:
- start with **separate ready queues by priority + delayed zset**
- it is easier to reason about and test than one overloaded structure

## B. RedisLeaseBackend / RedisLockBackend

Responsibilities:
- acquire task execution lease
- extend lease TTL
- release lease safely
- support worker-level coordination locks where needed

Requirements:
- ownership token per lease
- compare-and-delete / compare-and-extend safety
- test lease expiry and stolen lease behavior

## C. WorkerRegistry / Heartbeat subsystem

Responsibilities:
- register active workers
- update heartbeat timestamp / TTL
- expose liveness view
- support debugging and repair logic

Redis shape:
- worker:{id}:heartbeat
- optional worker metadata hash/set

## D. ExecutionAttempt store/model

Need durable modeling for:
- attempt_id
- task_id
- worker_id
- claim_time
- started_at
- completed_at
- terminal_status
- retry_count
- lease_owner_token (optional persisted copy)
- last_heartbeat_at (optional persisted snapshot)

This is critical. Without attempt records, distributed debugging and repair become hand-wavy.

## E. Completion Idempotency layer

Need to guarantee:
- repeated completion callbacks do not double-apply terminal side effects
- duplicate success/failure writes converge safely

Initial design:
- completion key based on task_id + attempt_id + terminal_state
- Redis fast dedupe with durable DB confirmation where necessary

## F. Distributed Reconciler v2

Expand reconciler from lightweight local repair to distributed repair service.

Responsibilities:
- find tasks stuck in claimed/running with expired lease
- detect worker heartbeat loss
- requeue retryable tasks
- mark terminally failed tasks when retry budget exhausted
- recover delayed tasks ready for promotion if scheduler path missed them
- repair schedule/task drift where relevant

Key rule:
- reconciler must be idempotent and safe to run concurrently
- use a global reconciler lock or sharded repair locks in Redis

## Data Model Changes

Need to evolve persistence models to represent distributed facts.

Likely additions:

### task table / model
- current_state_version (optional optimistic version)
- last_attempt_id
- next_retry_at
- claimed_by_worker
- lease_expires_at

### execution_attempt table
- attempt_id
- task_id
- worker_id
- status
- started_at
- completed_at
- error_summary
- retry_index
- heartbeat_at
- lease_token

### completion record / event table (optional but recommended)
- completion_key
- task_id
- attempt_id
- outcome
- processed_at
- handler_status

## API / Service Layer Evolution

Need service/container changes so the runtime can boot in distributed mode.

Additions:
- backend config for Redis connection and lease defaults
- worker identity configuration
- heartbeat interval / lease TTL / reconciler interval settings
- distributed-mode health endpoint data

Potential endpoints:
- worker registry status
- lease/debug status for a task
- execution attempts for task
- requeue / repair introspection endpoints

## Testing Strategy

This part is mandatory. No fake "distributed" milestone without it.

### Unit tests
- Redis backend queue semantics
- lease acquire/extend/release semantics
- completion idempotency logic
- reconciler repair decisions

### Integration tests
- two workers race to claim same task
- lease expires and second worker reclaims
- worker crashes mid-task and task is recovered
- duplicate completion delivery converges
- delayed task becomes ready and is executed

### End-to-end tests
- start Redis + multiple worker processes
- submit tasks and schedules
- verify distributed execution and recovery
- verify no permanent task loss under injected worker failure

## Rollout Plan (Design-Level)

### Phase 5 — Distributed substrate
- Redis queue backend
- Redis lease/lock backend
- backend factory/config integration
- initial tests

### Phase 6 — Worker ownership semantics
- worker identity + heartbeat subsystem
- claim/lease execution path
- execution attempt durable model
- recovery-aware executor flow

### Phase 7 — Completion and repair hardening
- completion idempotency
- distributed reconciler v2
- retry/requeue/expired-lease repair paths

### Phase 8 — Distributed validation
- multi-worker integration tests
- failure injection scenarios
- docs / deployment / operations notes

## Main Risks

1. **State split complexity**
   - Redis + DB can drift if transition boundaries are sloppy
   - mitigation: define explicit transition order and invariants

2. **Lease correctness bugs**
   - can cause double execution or stuck tasks
   - mitigation: token-based lease ownership, aggressive tests

3. **Overdesign too early**
   - trying to simulate a full distributed platform at once will slow delivery
   - mitigation: Redis-first, at-least-once only, simple queue model first

4. **Reconciler fights active workers**
   - mitigation: repair only on provably stale leases / dead workers

## Recommendation

Proceed with the aggressive Redis-first plan.

The first implementation milestone should optimize for:
- correctness of claim/lease/recovery semantics
- observability of distributed state
- deterministic testability

Not for:
- maximal throughput
- perfect abstraction purity
- strict exactly-once guarantees

## Approval Checkpoint

If approved, next step is to write the implementation task plan at task granularity for:
1. Redis distributed substrate
2. worker heartbeat / claim / lease flow
3. completion idempotency + distributed reconciler
4. distributed integration and fault-injection tests
