# Next-Phase Gap Analysis and Unified TODO Plan

Date: 2026-05-02
Project: ray-async-framework
Branch: feat/deepwiki-distributed-alignment

## Purpose

This document turns the current post-alignment state into an executable next-phase plan.

It is intentionally split by priority bands so the repository does not drift from a review-ready state while still keeping momentum toward stronger deepwiki parity.

---

## Current baseline

What is already materially in place:

- real Redis-backed critical shared-state paths for queue / lock / completion dedupe / worker liveness
- retry exhaustion convergence across executor / consumer / worker
- live Redis validation for smoke / recovery / overlap / retry exhaustion
- lease-focused observability endpoints and anomaly listing
- high-value fault-injection coverage for finalize / callback / reconciler overlap boundaries
- updated README / handoff / deepwiki reference / runtime deployment docs

This means the repository has crossed beyond a distributed-looking skeleton.

The remaining gaps are now mostly about:
- operational completeness
- runtime guidance depth
- stress / scale / failure-hardening
- boundary clarity for future architecture work

---

## Unified priority map

## P0 — Keep the branch review-ready while making the next step obvious

These items are low-risk, high-leverage, and should be done before heavier runtime work.

### P0.1 Single source of truth for gaps and sequencing
Status: **done in this document**

Why it matters:
- avoids fragmented TODOs across README / handoff / deepwiki notes
- gives a stable plan for the next work cycle

### P0.2 Keep docs and control-plane references in sync
Status: **partially done**

Remaining work:
- ensure every new observability endpoint is referenced consistently in README and handoff docs
- ensure runtime docs are discoverable from top-level docs

Recommendation:
- prefer docs sync as part of each future feature commit rather than a separate cleanup burst

### P0.3 Preserve a clean semantic story for reviewers
Status: **ongoing discipline task**

Rule:
- future changes should stay grouped by theme:
  - runtime guide / docs
  - observability
  - failure-injection / validation
  - runtime behavior / shared-state promotion

---

## P1 — High-value next implementation targets

These are the recommended next execution items for the repository.

### P1.1 Expand anomaly-oriented observability
Priority: **highest implementation priority**

Current state:
- `/debug/summary`
- `/debug/leases`
- `/debug/leases/{task_id}`
- `/debug/leases/anomalies`
- `/workers/{worker_id}/leases`

Remaining gaps:
- anomaly counts are not yet surfaced as a first-class summarized object beyond current lease fields
- no dedicated anomaly summary endpoint yet
- no operator-friendly grouping by anomaly type / worker / task status

Recommended next actions:
1. add grouped anomaly summary output
2. expose anomaly counts directly from `/debug/summary`
3. optionally add `/debug/leases/anomalies/summary`

Success criteria:
- an operator can answer “what is wrong right now?” without manually joining multiple endpoints

### P1.2 Deepen distributed runtime guidance into executable operational guidance
Priority: **high**

Current state:
- deployment guide exists
- backend config guidance exists
- fallback vs distributed behavior is documented

Remaining gaps:
- no concrete multi-process startup recipes
- no explicit Redis connection/pooling guidance
- no example deployment matrices by environment size

Recommended next actions:
1. add local multi-process recipe
2. add small-cluster deployment recipe
3. add TTL/heartbeat tuning examples by workload type
4. document reconciler cadence recommendations

Success criteria:
- a new engineer can run the system in distributed mode without reverse-engineering source code

### P1.3 Map non-critical shared-state boundaries to concrete code paths
Priority: **high**

Current state:
- conceptual boundary document exists

Remaining gaps:
- no explicit mapping from boundary categories to file/module lists
- no decision table for “promote to Redis vs keep local” by subsystem

Recommended next actions:
1. add a code-path appendix to the boundary doc
2. list candidate promotion targets and rationale
3. tag each candidate as keep-local / investigate / promote

Success criteria:
- future Redis-related work becomes scoped architecture work instead of open-ended churn

---

## P2 — Validation and runtime hardening

These items are important, but should follow P1 unless a productionization push starts immediately.

### P2.1 Long-running branch / DAG fault injection
Priority: **medium-high**

Recommended scenarios:
- worker death during long DAG branch execution
- heartbeat jitter during extended branch runtime
- partial branch completion with downstream repair expectations

### P2.2 Redis transient failure simulation
Priority: **medium-high**

Recommended scenarios:
- temporary Redis unavailability during heartbeat extend
- temporary failure during queue promotion or completion dedupe
- recovery behavior after transient backend errors

### P2.3 Heavier-load / stress validation
Priority: **medium**

Recommended scenarios:
- many concurrent workers consuming from shared queue
- delayed promotion under sustained concurrent load
- larger running-task sets with reconciler scans

Success criteria for P2 overall:
- confidence shifts from correctness-under-curated-tests toward correctness-under-load-and-jitter

---

## Recommended execution order

### Short version
1. **P1.1 anomaly-oriented observability expansion**
2. **P1.2 stronger runtime/deployment recipes**
3. **P1.3 code-path mapping for non-critical shared-state boundaries**
4. **P2 validation/hardening**

### Why this order
- P1.1 immediately improves operator usefulness
- P1.2 makes the repository easier to run in the intended distributed shape
- P1.3 reduces future architecture ambiguity
- P2 is valuable, but easiest to scope after the operator/runtime story is clearer

---

## What should not be done blindly next

Avoid immediately doing any of the following without a scoped decision:

- promoting every remaining local path into Redis
- adding more overlap tests without a new semantic target
- starting load testing without agreeing on target deployment shape
- widening the branch into broad refactors unrelated to distributed semantics

---

## Immediate next recommended task

If continuing right away, the best next concrete task is:

> **Expand anomaly-oriented observability by adding grouped anomaly counts / summary output, and wire that summary into `/debug/summary`.**

This is the cleanest continuation of the work already landed in:
- lease observability endpoints
- anomaly endpoint
- runtime guidance docs

It is small enough to review, operationally useful, and aligned with the current branch theme.
