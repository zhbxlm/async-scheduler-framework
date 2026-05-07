# P6 Technical Debt & Hardening Plan

Date: 2026-05-07
Status: Draft
Depends on:
- `docs/plans/2026-05-07-p6-tech-debt-hardening-design.md`

---

## Execution order

1. Extract `_maybe_await` to common module
2. Implement real force-lease-eviction
3. Add replay rate limiting
4. Governance config loading
5. RecoveryExplainer model binding tightening

---

# Phase 1 — DRY: common async db helpers ✅→

## Task 1.1 — Add `src/common/db_utils.py` with `maybe_await`
## Task 1.2 — Replace all local `_maybe_await` defs across service files
## Task 1.3 — Verify all tests still pass

---

# Phase 2 — Real force-lease-eviction

## Task 2.1 — Wire Redis key deletion into force-lease-eviction endpoint
## Task 2.2 — Add Redis eviction confirmation to response
## Task 2.3 — Add force-lease-eviction Redis integration test

---

# Phase 3 — Replay rate limiting

## Task 3.1 — Add replay count check in `ReplayPolicyService`
## Task 3.2 — Query `operator_actions` within sliding window
## Task 3.3 — Add rate limit config (window_seconds, max_replays)
## Task 3.4 — Add rate limiting tests

---

# Phase 4 — Governance config loading

## Task 4.1 — Accept optional config dict in `GovernanceService`
## Task 4.2 — Default to existing hardcoded set when no config
## Task 4.3 — Add config override tests

---

# Phase 5 — RecoveryExplainer model binding

## Task 5.1 — Import and reference actual `TaskRecord` fields
## Task 5.2 — Remove `getattr` fallbacks for known columns
## Task 5.3 — Update explainer tests

---

# Final verification

## V1 — 519+ tests still passing after DRY refactor
## V2 — force-lease-eviction confirms Redis key deleted
## V3 — replay denied after rate limit exceeded
## V4 — governance config override works
## V5 — recovery explainer references real model fields
