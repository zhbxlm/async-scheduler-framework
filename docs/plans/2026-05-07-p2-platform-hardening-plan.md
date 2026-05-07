# P2 Platform Hardening Implementation Plan

Date: 2026-05-07
Status: Draft
Depends on:
- `docs/plans/2026-05-07-p2-platform-hardening-design.md`

---

## Execution order

Recommended P2 sequence:
1. TaskRun / DagRun maturation
2. callback operations maturity
3. broader service-layer extraction
4. lease / replay / repair contract hardening
5. audit / timeline / observability

---

# Phase 1 — TaskRun / DagRun maturation

## Task 1.1 — Expand TaskRun to represent real attempt lineage
## Task 1.2 — Introduce DagRunRecord (minimal viable shape)
## Task 1.3 — Link task creation / execution paths to TaskRun creation
## Task 1.4 — Record worker / lease / attempt metadata on runs
## Task 1.5 — Add run-centric query tests

---

# Phase 2 — Callback operations maturity

## Task 2.1 — Add dead-letter inspection query path
## Task 2.2 — Add callback replay / resend operation
## Task 2.3 — Add callback retry policy metrics
## Task 2.4 — Add ops-facing callback summary endpoints
## Task 2.5 — Add alert thresholds for backlog / dead-letter growth

---

# Phase 3 — Broader service-layer extraction

## Task 3.1 — Extract schedule application service
## Task 3.2 — Extract capability application service
## Task 3.3 — Extract tenant application service
## Task 3.4 — Extract DAG application service
## Task 3.5 — Reduce routes to transport-only shape across these domains

---

# Phase 4 — Lease / replay / repair hardening

## Task 4.1 — Formalize lease lifecycle contract in code
## Task 4.2 — Add explicit replay operation model
## Task 4.3 — Add repair decision matrix tests
## Task 4.4 — Add operator intervention boundaries and safeguards

---

# Phase 5 — Audit / timeline / observability

## Task 5.1 — Introduce task event model
## Task 5.2 — Emit lifecycle / callback / repair events
## Task 5.3 — Add timeline query API
## Task 5.4 — Add basic operator observability summaries
## Task 5.5 — Sync docs and operations runbook

---

# Final verification

## V1 — end-to-end run lineage scenarios
## V2 — callback replay / dead-letter scenarios
## V3 — service-layer extraction regression suite
## V4 — timeline / audit visibility checks
