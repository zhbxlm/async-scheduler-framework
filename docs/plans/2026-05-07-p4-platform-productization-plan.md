# P4 Platform Productization Plan

Date: 2026-05-07
Status: Completed
Depends on:
- `docs/plans/2026-05-07-p4-platform-productization-design.md`

---

## Execution order

Recommended P4 sequence:
1. operator experience surface
2. recovery explainability
3. replay safety policies
4. run-centric views
5. governance / risk controls

---

# Phase 1 — Operator experience surface

## Task 1.1 — Add operator summary query service
## Task 1.2 — Add callback backlog / dead-letter summary views
## Task 1.3 — Add recent operator action views
## Task 1.4 — Add recent timeline views
## Task 1.5 — Add tests for summary/filtering surfaces

---

# Phase 2 — Recovery explainability

## Task 2.1 — Add stale recovery explanation service
## Task 2.2 — Explain lease state / last repair attempt / operator interventions
## Task 2.3 — Expose recovery explanation endpoint
## Task 2.4 — Add explainability tests

---

# Phase 3 — Replay safety policies

## Task 3.1 — Add replay safety check service
## Task 3.2 — Require explicit reason for risky replay
## Task 3.3 — Deny replay under active valid lease
## Task 3.4 — Add callback replay vs task replay policy split
## Task 3.5 — Add policy tests

---

# Phase 4 — Run-centric views

## Task 4.1 — Add task→runs query service
## Task 4.2 — Add dag→runs query service
## Task 4.3 — Add replay lineage chain query
## Task 4.4 — Expose run-centric ops endpoints

---

# Phase 5 — Governance / risk controls

## Task 5.1 — Define intervention role categories in code/docs
## Task 5.2 — Mark high-risk operations
## Task 5.3 — Add stronger audit requirements for force operations
## Task 5.4 — Add governance tests / docs sync

---

# Final verification

## V1 — operator dashboard/query scenarios
## V2 — replay safety denial/allow scenarios
## V3 — run-centric lineage scenarios
## V4 — force-operation governance scenarios
