# Deepwiki Distributed Architecture Reference

Date: 2026-05-01
Project: async-scheduler-framework
Status: Working reference

## Purpose

This document consolidates the deepwiki-style distributed scheduler architecture
that the repository is converging toward, and maps it to what is already built
in this codebase.

It is not a product spec. It is an engineering reference for future iteration,
code review, and gap tracking.

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
- current backend is Redis-shaped / process-local
- not yet backed by real external Redis server state

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
- token-safe acquire / extend / release works
- current lock backend is Redis-shaped / process-local

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
- current backend is Redis-shaped / process-local

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
- default runtime can use Redis-shaped shared dedupe semantics in-process
- still not real external Redis-backed shared state

### 2.6 Repair semantics

Expected behavior:
- stale running task with dead worker + dead lease gets repaired
- live worker / live lease task must not be stolen
- concurrent reconcilers must not double-repair same task
- repair must update durable task and attempt records

Current repository mapping:
- `async_scheduler.platform.reconciler.TaskReconciler`

Current status:
- distributed repair v2 minimal version implemented
- reconciler repair lock implemented
- stale-task requeue and abandoned-attempt marking implemented

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
- two-worker claim race converges to single executor
- heartbeat updates running attempt state
- duplicate completion converges safely
- stale leased task can be requeued by reconciler
- active leased task is not stolen
- two reconcilers do not double-repair same task
- multi-worker integration path works
- dead-worker recovery path works
- lease-loss path ends in controlled failure

---

## 5. Remaining Gaps To Reach Stronger Deepwiki Parity

### 5.1 Real Redis-backed shared state

Still needed for stronger parity:
- real Redis client wiring
- shared queue keys in Redis
- shared lease keys in Redis
- shared worker heartbeat keys in Redis
- shared completion dedupe keys in Redis

### 5.2 Stronger failure injection

Recommended additional scenarios:
- lease lost during callback dispatch
- duplicated completion + reconciler overlap
- worker death during long DAG branch execution
- delayed task promotion under concurrent workers
- partial recovery with retry exhaustion

### 5.3 Control-plane observability

Useful future additions:
- worker listing endpoint
- attempt inspection endpoint
- repair audit log view
- lease / heartbeat debug endpoint

---

## 6. Repository Status Summary

This repository has already crossed from:

- local framework with distributed abstractions

into:

- a working distributed-kernel skeleton with recovery semantics

But it has not fully crossed into:

- real external Redis-backed multi-process shared-state implementation

So the correct description today is:

> deepwiki-aligned distributed execution kernel skeleton with tested claim,
> lease, liveness, attempt, idempotency, and repair semantics; current Redis
> components remain Redis-shaped stand-ins pending real shared-state wiring.

---

## 7. Recommended Next Iteration

1. replace Redis-shaped backends with real redis-py backed implementations
2. keep current tests as semantic contract tests
3. add stronger fault injection matrix
4. add operational observability endpoints
5. document runtime configuration for true distributed deployment
