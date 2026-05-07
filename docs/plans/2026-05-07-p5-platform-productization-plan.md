# P5 Platform Productization Plan

Date: 2026-05-07
Status: Completed

Depends on:
- `docs/plans/2026-05-07-p5-platform-productization-design.md`
- `docs/plans/2026-05-07-p4-platform-productization-plan.md` (predecessor)

---

## Summary

P5 completed all remaining productization items building on P4's foundations.
All 5 phases executed in sequence; 519/519 tests passing at completion.

---

## Execution order

1. operator UX aggregation views
2. callback replay policy (separated from task replay)
3. force operation governance enforcement
4. replay chain query
5. governance contract documentation

---

# Phase 1 — Operator UX aggregation views ✅

## Delivered
- `src/services/operator_ux.py` — `OperatorUXService`
  - `stale_task_queue()` — running task runs without recovery
  - `replay_queue()` — replay-requested runs
  - `dead_letter_backlog()` — unacked dead-letter callbacks
  - `recent_operator_actions()` — filterable operator action history
- Endpoints: `/ops/v1/dashboard/stale-queue`, `/replay-queue`, `/dead-letter-backlog`, `/recent-actions`
- Tests: `tests/test_operator_ux.py`

---

# Phase 2 — Callback replay policy ✅

## Delivered
- `src/services/callback_replay_policy.py` — `CallbackReplayPolicyService`
  - Deny replay when callback is already `pending`
  - Deny replay of `delivered` callbacks for non-admin
  - Admin override for `delivered` callback replay
  - Role check via `GovernanceService`
- Integrated into `POST /ops/v1/callbacks/dead-letters/{id}/replay`
- Tests: `tests/test_callback_replay_policy.py`

---

# Phase 3 — Force operation governance enforcement ✅

## Delivered
- `src/services/force_operations.py` — `ForceOperationService`
  - Role check (admin required for high-risk ops)
  - Reason required for high-risk ops
  - Durable operator audit via `OperatorActionService`
- Endpoint: `POST /ops/v1/tasks/{task_id}/force-lease-eviction`
- Tests: `tests/test_force_operations.py`

---

# Phase 4 — Replay chain query ✅

## Delivered
- `src/services/replay_chain_queries.py` — `ReplayChainQueryService`
  - `get_replay_chain(task_id)` — ordered list of all runs by creation time
- Endpoint: `GET /ops/v1/tasks/{task_id}/replay-chain`
- Tests: `tests/test_replay_chain_queries.py`

---

# Phase 5 — Governance contract documentation ✅

## Delivered
- `docs/plans/2026-05-07-governance-contract.md`
  - Role categories: operator / admin
  - High-risk operation table
  - Audit requirements table
  - Policy enforcement points table
  - Known limitations

---

# Final verification results

## V1 — operator UX aggregation ✅
All 4 dashboard queue/action views wired and tested.

## V2 — callback replay policy ✅
5 policy scenarios covered (reason missing, pending, delivered operator, delivered admin, dead-letter operator).

## V3 — force-operation governance ✅
4 force operation scenarios covered (insufficient role, no reason, admin+reason, operator ordinary).

## V4 — replay chain ✅
Run chain query returns ordered lineage for a task.
