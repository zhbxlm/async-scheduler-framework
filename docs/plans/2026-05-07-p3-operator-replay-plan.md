# P3 Operator & Replay Productization Plan

Date: 2026-05-07
Status: Completed
Depends on:
- `docs/plans/2026-05-07-p3-operator-replay-design.md`

---

## Execution order

Recommended P3 sequence:
1. operator action model
2. replay lineage model
3. dead-letter productization
4. eventized timeline completion
5. lease / stale recovery operator controls

---

# Phase 1 — Operator action model

## Task 1.1 — Add OperatorActionRecord
## Task 1.2 — Add OperatorAction service
## Task 1.3 — Record replay / cancel / dead-letter actions durably
## Task 1.4 — Add tests for operator action persistence

---

# Phase 2 — Replay lineage model

## Task 2.1 — Add replay operation service
## Task 2.2 — Replay creates new TaskRun lineage
## Task 2.3 — Preserve original run immutably
## Task 2.4 — Emit replay timeline events

---

# Phase 3 — Dead-letter productization

## Task 3.1 — Add dead-letter acknowledgement state
## Task 3.2 — Add replay history for callbacks
## Task 3.3 — Add replay safety checks
## Task 3.4 — Add ops endpoints for ack + replay history

---

# Phase 4 — Eventized timeline completion

## Task 4.1 — Emit operator action events
## Task 4.2 — Emit callback lifecycle events
## Task 4.3 — Emit run replay lineage events
## Task 4.4 — Add timeline query enhancements / filters

---

# Phase 5 — Lease / recovery operator controls

## Task 5.1 — Add stale recovery explanation query
## Task 5.2 — Add forced recovery operation model
## Task 5.3 — Add operator-safe recovery guards
## Task 5.4 — Add tests for replay vs repair decision rules

---

# Final verification

## V1 — operator action audit scenarios
## V2 — replay lineage scenarios
## V3 — dead-letter ack + replay scenarios
## V4 — stale recovery operator scenarios
