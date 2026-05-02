# Deepwiki Distributed Architecture Reference

Date: 2026-05-02
Project: async-scheduler-framework
Status: Updated to reflect current progress

## Purpose

This document consolidates the deepwiki-style distributed scheduler architecture
that the repository is converging toward, and maps it to what is already built
in this codebase.

It is not a product spec. It is an engineering reference for future iteration,
code review, and gap tracking.

## Key Updates Since Last Version

- **Lua/CAS‑style atomicity**: lock release/extend and queue promote/reprioritize/cancel operations now have
  Redis‑backed Lua‑atomic implementations.
- **live Redis verification**: optional live‑Redis smoke, recovery, overlap, multi‑worker race, and end‑to‑end
  consumer‑loop tests are now part of the repository (run when `TEST_REDIS_URL` is set).
- **attempt‑task consistency**: completion path now converges the latest non‑terminal execution attempt together
  with task terminalization; consumer, reconciler, and completion are aligned.
- **observability expansion**: worker listing & detail, latest‑attempt, attempt history, repair‑history filtering,
  health/queue‑stats improvements, and a unified `/debug/summary` endpoint are now available.
- **semantic refinement**: lease‑loss path now leads to `TaskStatus.FAILED` and `ExecutionAttemptStatus.ABANDONED`;
  ordinary retry‑budget exhaustion now converges to `TaskStatus.FAILED` and `ExecutionAttemptStatus.FAILED`.

These updates move the repository from a **pure transition‑state skeleton** to a **partial real‑Redis‑backed
kernel with validated concurrency/recovery semantics**.

---

## 1. Target System Shape

The target runtime is a distributed async scheduling kernel with:

- shared queue coordination
- lease-based task ownership
- worker liveness tracking
- durable execution-attempt history
- idempotent completion processing
- reconciler-driven stale task repair
- multi-worker recovery semantics

The delivery goal is **at-least-once with convergence**, not strict exactly-once.

---

## 2. Core Distributed Semantics

### 2.1 Queue semantics

Expected behavior:
- tasks are published to a shared queue
- delayed tasks become visible only after ready time
- higher priority tasks are consumed first
- requeue/cancel/stat inspection are observable

Current repository mapping:
- `async_scheduler.backends.redis.RedisQueueBackend`
- `async_scheduler.queue.QueueManager`

Current status:
- queue contract is implemented
- **queue promote, reprioritize, cancel operations are backed by Lua‑atomic Redis‑backed implementation**
- queue size, scheduled membership, task payload storage, and delayed‑task promotion are backed by real Redis keys
- **opt‑in live Redis verification passes multi‑worker race and delayed‑promotion scenarios**
- process‑local state is retained only as a deterministic fallback path when no Redis client is available

### 2.2 Ownership semantics

Expected behavior:
- worker must claim task ownership before execution
- ownership is expressed as a lease with token + ttl
- only current owner may extend/release lease
- expired lease makes task eligible for recovery/reclaim

Current repository mapping:
- `async_scheduler.backends.redis.RedisLockBackend`
- `async_scheduler.core.consumer.TaskConsumer`

Current status:
- lease contract is implemented
- **release and extend operations are backed by Lua‑atomic Redis‑backed implementation**
- **opt‑in live Redis verification passes lease‑loss, duplicate‑completion, and dead‑owner recovery scenarios**
- token‑safe acquire / extend / release / is_locked work with real Redis keys
- process‑local lease state is retained only as a deterministic fallback path when no Redis client is available

### 2.3 Worker liveness semantics

Expected behavior:
- workers register externally
- workers periodically heartbeat
- dead workers become externally detectable by ttl expiry
- repair logic can rely on worker liveness view

Current repository mapping:
- `async_scheduler.distributed.worker_registry.WorkerRegistry`
- `WorkerInfo`

Current status:
- registration / heartbeat / stale detection implemented
- worker liveness keys are backed by real Redis hashes + ttl when a Redis client is available
- process‑local worker state is retained only as a deterministic fallback path when no Redis client is available

### 2.4 Execution attempt semantics

Expected behavior:
- every distributed execution claim produces durable attempt data
- attempts record worker, retry index, lease token, timing, terminal status
- stale or failed attempts remain inspectable for recovery/debugging

Current repository mapping:
- `async_scheduler.core.models.ExecutionAttempt*`
- `async_scheduler.persistence.models.ExecutionAttemptORM`
- `ExecutionAttemptRepository`

Current status:
- durable attempt modeling implemented
- attempt claim / running / terminal transitions are persisted
- latest-attempt convergence rules are now partially enforced by completion + consumer paths
- important semantic split is preserved:
  - task `FAILED` after executor retry-budget exhaustion corresponds to attempt `FAILED`
  - task `FAILED` due to lease loss corresponds to attempt `ABANDONED`
  - task `SUCCESS` should converge latest non-terminal attempt to `SUCCEEDED`

### 2.5 Completion semantics

Expected behavior:
- duplicate terminal completion delivery converges safely
- callback and completion handlers should not double-apply side effects
- completion dedupe should ideally work across workers/processes

Current repository mapping:
- `async_scheduler.platform.completion.TaskCompletionNode`
- `RedisCompletionDedupBackend`

Current status:
- idempotent completion semantics implemented
- dedupe backend is injectable
- **completion‑dedup keys are backed by real Redis storage**
- completion path can optionally converge the latest non‑terminal execution attempt together with task terminalization
- **opt‑in live Redis verification passes duplicate‑completion, completion‑overlap, and multi‑worker race scenarios**
- process‑local dedupe state is retained only as a deterministic fallback path when no Redis client is available

### 2.6 Repair semantics

Expected behavior:
- stale running task with dead worker + dead lease gets repaired
- live worker / live lease task must not be stolen
- concurrent reconcilers must not double‑repair same task
- repair must update durable task and attempt records

Current repository mapping:
- `async_scheduler.platform.reconciler.TaskReconciler`

Current status:
- distributed repair v2 minimal version implemented
- reconciler repair lock implemented
- stale‑task requeue and abandoned‑attempt marking implemented
- **repair audit history (up to 1000 entries) is now available via `/reconciler/history` endpoint with filtering by action/task_id**

---

## 3. Execution Lifecycle Mapping

Target lifecycle:

1. task persisted
2. task enqueued
3. worker dequeues candidate
4. worker acquires lease
5. execution attempt created
6. task enters running
7. heartbeat loop renews lease + records attempt heartbeat
8. task completes or fails
9. completion pipeline applies terminal transition idempotently
10. reconciler repairs abnormal stale states

Current code mapping:
- enqueue/dequeue: `QueueManager` + `RedisQueueBackend`
- claim/lease: `TaskConsumer` + `RedisLockBackend`
- attempts: `ExecutionAttemptRepository`
- heartbeat loop: `TaskConsumer._heartbeat_loop`
- completion: `TaskCompletionNode.finalize`
- repair: `TaskReconciler.reconcile`

---

## 4. What Is Already Proven By Tests

Covered today by repository tests:

- queue priority / delay / cancel / stats
- lock acquire / extend / release / expiry semantics
- worker heartbeat / stale detection
- execution attempt persistence lifecycle
- two‑worker claim race converges to single executor
- heartbeat updates running attempt state
- duplicate completion converges safely
- stale leased task can be requeued by reconciler
- active leased task is not stolen
- two reconcilers do not double‑repair same task
- multi‑worker integration path works
- dead‑worker recovery path works
- lease‑loss path ends in controlled failure (`TaskStatus.FAILED` + `ExecutionAttemptStatus.ABANDONED`)
- latest‑attempt convergence for success / retry / lease‑loss paths
- completion / reconciler overlap convergence semantics
- duplicate completion / reconciler overlap convergence semantics
- **live Redis verification suite now includes:**
  - smoke (atomic lock/queue Lua operations)
  - recovery (dead worker, dead lease)
  - overlap (completion + reconciler races)
  - duplicate completion + reconciler overlap
  - lease loss + final‑completion race
  - attempt‑consistency overlap (reconciler marks `ABANDONED` first)
  - multi‑worker + duplicate‑completion overlap
  - delayed‑promotion + multi‑worker race
  - dead‑owner + new‑worker recovery
  - end‑to‑end consumer loop (queue → lock → dedup → registry → reconciler)
  - external‑job crash & recovery
  - retry‑budget exhaustion converging to terminal failure without requeue

---

## 5. Remaining Gaps To Reach Stronger Deepwiki Parity

### 5.1 Real Redis-backed shared state

Progress since last version:
- **critical lock/queue Lua‑atomic paths are now backed by real Redis keys**
- **worker heartbeat keys are already backed by real Redis keys**
- **completion dedupe keys are already backed by real Redis keys**
- **live Redis verification suite provides confidence that these paths work under real Redis**

Remaining gaps to stronger parity:
- real Redis client wiring for remaining non‑critical runtime paths outside the critical distributed kernel
- complete replacement of remaining Redis-shaped fallback-oriented code paths where true shared state is still desired

### 5.2 Stronger failure injection

Progress since last version:
- **lease lost during callback dispatch is now covered**
- **duplicate finalize/completion overlap + callback failure is now covered**
- **concurrent reconciler overlap without double requeue is now covered**
- **callback failure + lease loss + reconciler overlap is now covered**
- **partial recovery with retry exhaustion is now covered**

Remaining recommended scenarios:
- Redis transient failure / network partition style simulation beyond current control-point jitter coverage
- sustained delayed-task promotion under heavier concurrent load
- broader queue-side transient failure matrix outside reconciler requeue commit-point protection
- edge-case DAG semantics (partial branch success with downstream cancellation vs. failure interplay, fan‑out/fan‑in under interruption, DAG‑level timeout/deadline enforcement)

### 5.3 Control-plane observability

Progress since last version:
- **worker listing endpoint (`/workers`) with detail (`/workers/{id}`)**
- **attempt inspection endpoints (`/tasks/{id}/attempts/latest`, `/tasks/{id}/attempts`)**
- **repair audit log view (`/reconciler/history`) with filtering by action/task_id**
- **unified debug summary endpoint (`/debug/summary`) that aggregates health, queue, workers, reconciler, and lease anomaly signals**
- **lease / heartbeat debug endpoints are now available:**
  - `/debug/leases`
  - `/debug/leases/{task_id}`
- **worker‑specific current lease / attempt listing is now available:**
  - `/workers/{worker_id}/leases`

Useful future additions:
- anomaly-focused lease/debug endpoint (e.g. suspicious states only)
- real‑time queue depth / lease change monitoring (WebSocket/SSE)
- stronger worker / task / lease correlation summaries for operators

---

## 6. Repository Status Summary

This repository has already crossed from:

- local framework with distributed abstractions

into:

- a working distributed‑kernel skeleton with recovery semantics
- a **partial real‑Redis transition state with atomic lock/queue critical paths and opt‑in live Redis verification**
- **a verified concurrency matrix covering duplicate‑completion, lease‑loss, dead‑owner recovery, multi‑worker race, and end‑to‑end consumer loop scenarios**

But it has not fully crossed into:

- real external Redis‑backed multi‑process shared‑state implementation for all non‑critical paths

So the correct description today is:

> deepwiki‑aligned distributed execution kernel with tested claim,
> lease, liveness, attempt, idempotency, repair, and attempt‑consistency semantics;
> the repository now includes **real Redis‑backed critical shared-state paths,
> Lua/CAS‑style atomicity for lock/queue operations, opt‑in live Redis verification
> covering overlap/recovery/multi‑worker/delayed‑promotion/retry‑exhaustion scenarios,
> and a growing control‑plane observability layer** – but it is not yet a fully
> production‑grade distributed runtime across every non‑critical path.

---

## 7. Recommended Next Iteration

1. **document true distributed deployment/runtime guidance**
   - multi-process / multi-node topology
   - Redis connection / pooling expectations
   - lease TTL / heartbeat tuning guidance
   - fallback mode vs real Redis mode behavior
2. **clarify remaining non-critical shared-state gaps**
   - identify which fallback-oriented paths should stay local
   - identify which should be promoted to true shared-state Redis paths
3. keep current tests as semantic contract tests and **extend live Redis verification / stress coverage**
4. **expand operational observability**
   - anomaly-oriented lease views are now in place; continue with richer operator summaries
   - optional real-time monitoring
5. **strengthen the remaining fault-injection matrix**
   - Redis transient failure / partition-like simulation beyond current heartbeat / completion / reconciler / requeue control points
   - heavier-load delayed-promotion / concurrency stress
