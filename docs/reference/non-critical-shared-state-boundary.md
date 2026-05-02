# Non-Critical Shared-State Boundary

Date: 2026-05-02
Project: async-scheduler-framework

## Purpose

This document clarifies which parts of the repository already need **true shared state** for distributed correctness, which parts can remain local for now, and which parts should be revisited later.

The goal is to avoid two common mistakes:

1. over-promoting every path into Redis even when local state is acceptable
2. under-promoting paths that actually affect distributed correctness

---

## 1. Critical shared-state paths

These paths are already part of the distributed kernel and should be treated as shared-state correctness boundaries.

### Queue coordination
Why critical:
- multiple workers must see the same task visibility/order state
- delayed promotion must converge across workers

Current implementation:
- `RedisQueueBackend`

### Lease / ownership
Why critical:
- only one worker should own a task lease at a time
- expired ownership must become externally visible

Current implementation:
- `RedisLockBackend`

### Completion dedupe
Why critical:
- duplicate finalize/completion races must converge across processes

Current implementation:
- `RedisCompletionDedupBackend`

### Worker liveness
Why critical:
- reconciler repair depends on externally visible worker heartbeat state

Current implementation:
- `WorkerRegistry`

---

## 2. Durable but not necessarily Redis-shared paths

These paths are still important, but they do not necessarily require Redis as the immediate source of truth.

### Task records / execution attempts
Why not identical to queue/lease state:
- these are durable records used for audit, reconciliation, and debugging
- correctness depends more on persistence and convergence than on Redis specifically

Current implementation:
- DB-backed repositories / ORM models

### Schedule registry
Why not currently promoted:
- current distributed kernel effort is centered on task execution correctness
- schedule lifecycle correctness is important, but not yet the primary shared-state bottleneck

Current implementation:
- local persistence oriented

---

## 3. Local control-plane paths that are acceptable for now

These paths can remain local or API-assembled for now because they primarily support operator visibility rather than execution correctness.

### Debug summaries
Examples:
- `/debug/summary`
- `/debug/leases`
- `/workers/{worker_id}/leases`

Why acceptable:
- they aggregate state for humans
- they do not decide lease ownership or task completion semantics

### README / PR handoff / engineering reference docs
Why acceptable:
- documentation is not runtime shared state
- correctness comes from code + tests, not shared storage

---

## 4. Candidate paths to revisit later

These are not urgent blockers for current distributed correctness, but they are worth revisiting as the repository moves toward stronger production parity.

### Schedule coordination semantics
Questions to revisit:
- should active schedule ownership ever be externally coordinated?
- should schedule dedupe / fire advancement become more explicitly shared-state aware?

### Callback / outbound delivery workflow
Questions to revisit:
- should outbound callback delivery use a durable queue?
- should delivery retries be modeled separately from task terminalization?

### Rich operator state views
Questions to revisit:
- should anomaly summaries be cached or externally materialized?
- should worker/task/lease correlation views have their own shared snapshot model?

---

## 5. Decision heuristic

When deciding whether a path should be promoted from local/fallback-oriented behavior into true shared state, use this rule:

### Promote it if all are true
- multiple workers/processes can mutate or observe it concurrently
- incorrect divergence can cause duplicate execution, lost ownership, or incorrect recovery
- humans cannot safely repair it after the fact without correctness impact

### It can remain local for now if most are true
- it is mostly diagnostic or control-plane assembly
- divergence affects visibility more than execution correctness
- durable DB state already captures the authoritative history

---

## 6. Code-path mapping appendix

### Critical shared-state code paths

| Area | Current modules / files | Recommendation |
|------|--------------------------|----------------|
| Queue coordination | `async_scheduler/backends/redis.py`, `async_scheduler/queue/*` | **promote / keep shared** |
| Lease / ownership | `async_scheduler/backends/redis.py`, `async_scheduler/core/consumer.py` | **promote / keep shared** |
| Completion dedupe | `async_scheduler/platform/completion.py`, `async_scheduler/platform/completion.py`, Redis-backed dedupe wiring | **promote / keep shared** |
| Worker liveness | `async_scheduler/distributed/worker_registry.py` | **promote / keep shared** |

### Durable but not necessarily Redis-shared code paths

| Area | Current modules / files | Recommendation |
|------|--------------------------|----------------|
| Task persistence | `async_scheduler/persistence/models.py`, `async_scheduler/persistence/repositories.py` | **keep durable DB-backed** |
| Attempt persistence | `async_scheduler/persistence/models.py`, `async_scheduler/persistence/repositories.py` | **keep durable DB-backed** |
| Schedule registry | `async_scheduler/scheduler/*`, `RegistryBackend` paths | **investigate before promote** |

### Local / control-plane aggregation code paths

| Area | Current modules / files | Recommendation |
|------|--------------------------|----------------|
| Debug summaries | `async_scheduler/api/app.py` (`/debug/summary`) | **keep local aggregation** |
| Lease anomaly views | `async_scheduler/api/app.py` (`/debug/leases*`) | **keep local aggregation; improve operator UX** |
| PR / review docs | `README.md`, `PR_READY_NOTES.md`, `docs/reference/*` | **keep local/documentation only** |

## 7. Decision table

| Candidate path | Current state | Next action | Rationale |
|----------------|--------------|------------|-----------|
| Queue / lease / dedupe / liveness | already Redis-backed | **keep shared** | correctness-critical under concurrency |
| Task / attempt DB persistence | durable DB-backed | **keep as-is for now** | authority is durability + convergence, not Redis mirroring |
| Schedule registry lifecycle | local-persistence oriented | **investigate** | may need future distributed semantics, but not current kernel blocker |
| Debug / anomaly summaries | API-assembled | **keep local** | operator visibility, not execution correctness |
| Callback/outbound delivery workflow | lightweight | **investigate** | may eventually need durable outbound queue |

## 8. Current recommendation

For the current repository stage:

### Keep treating as critical shared-state
- queue
- lock / lease
- completion dedupe
- worker liveness

### Keep as durable-but-not-yet-Redis-promoted
- task / attempt persistence
- schedule registry

### Keep as local/control-plane aggregation
- debug summaries
- observability assembly endpoints
- review / handoff documents

This boundary is a deliberate design choice, not an accident.

The next iterations should only promote more paths when there is a clear correctness or operational benefit.
