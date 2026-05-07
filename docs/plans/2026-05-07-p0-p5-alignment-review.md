# Architecture Review: P0–P5 Document & Code Alignment

Date: 2026-05-07

---

## 1. Document Status

| Document | Status | Aligned to code |
|---|---|---|
| p0-architecture-remediation-design.md | Active | ✅ |
| p0-architecture-remediation-plan.md | Completed | ✅ |
| p1-architecture-evolution-design.md | Active | ✅ |
| p1-architecture-evolution-plan.md | Completed | ✅ |
| p2-platform-hardening-design.md | Active | ✅ |
| p2-platform-hardening-plan.md | Completed | ✅ |
| p3-operator-replay-design.md | Active | ✅ |
| p3-operator-replay-plan.md | Completed | ✅ |
| p4-platform-productization-design.md | Active | ✅ |
| p4-platform-productization-plan.md | Completed | ✅ |
| p5-platform-productization-design.md | Active | ✅ |
| p5-platform-productization-plan.md | Completed | ✅ |
| governance-contract.md | Active | ✅ |

---

## 2. Service Inventory

### New services added across P0–P5

| Service | Layer | Phase | Purpose |
|---|---|---|---|
| `task_state_machine.py` | platform | P1 | Explicit task lifecycle transitions |
| `callback_dispatcher.py` | services | P1 | Durable callback dispatch |
| `task_application.py` | services | P1 | Task submission/cancellation/result |
| `run_tracking.py` | services | P2 | DagRun/TaskRun creation |
| `task_timeline.py` | services | P2 | Task event emission |
| `callback_ops.py` | services | P2 | Dead-letter list/ack/replay |
| `task_audit_queries.py` | services | P2 | Timeline/callback summary queries |
| `operator_actions.py` | services | P3 | Durable operator action recording |
| `operator_queries.py` | services | P3 | Operator action list/filter |
| `replay_lineage.py` | services | P3 | Task replay creates new run lineage |
| `operator_dashboard.py` | services | P4 | Aggregated dashboard summary |
| `recovery_explainer.py` | services | P4 | Stale task explanation |
| `replay_policy.py` | services | P4 | Task replay safety checks |
| `run_centric_queries.py` | services | P4 | Task/dag run queries |
| `governance.py` | services | P4 | Role/risk classification |
| `operator_ux.py` | services | P5 | Operator UX aggregation queues |
| `callback_replay_policy.py` | services | P5 | Callback-specific replay policy |
| `force_operations.py` | services | P5 | High-risk force operation enforcement |
| `replay_chain_queries.py` | services | P5 | Task run replay lineage chain |

### New models added across P0–P5

| Model | Phase | Purpose |
|---|---|---|
| `callback_outbox.py` | P1 | Persistent outbound callback records |
| `dag_run.py` | P2 | DAG execution instance records |
| `task_event.py` | P2 | Task timeline event records |
| `operator_action.py` | P3 | Operator intervention audit records |
| `task_run.py` | P5 | Task execution instance records |

---

## 3. Ops API Surface

| Endpoint | Method | Phase | Service |
|---|---|---|---|
| `/ops/v1/dashboard/summary` | GET | P4 | `OperatorDashboardService` |
| `/ops/v1/dashboard/stale-queue` | GET | P5 | `OperatorUXService` |
| `/ops/v1/dashboard/replay-queue` | GET | P5 | `OperatorUXService` |
| `/ops/v1/dashboard/dead-letter-backlog` | GET | P5 | `OperatorUXService` |
| `/ops/v1/dashboard/recent-actions` | GET | P5 | `OperatorUXService` |
| `/ops/v1/callbacks/summary` | GET | P2 | `TaskAuditQueryService` |
| `/ops/v1/callbacks/dead-letters` | GET | P2 | `CallbackOpsService` |
| `/ops/v1/callbacks/dead-letters/{id}/ack` | POST | P3 | `CallbackOpsService` |
| `/ops/v1/callbacks/dead-letters/{id}/replay` | POST | P3/P5 | `CallbackOpsService` + `CallbackReplayPolicyService` |
| `/ops/v1/tasks/{id}/timeline` | GET | P2 | `TaskAuditQueryService` |
| `/ops/v1/tasks/{id}/runs` | GET | P4 | `RunCentricQueryService` |
| `/ops/v1/tasks/{id}/runs/{run_key}/events` | GET | P4 | `RunCentricQueryService` |
| `/ops/v1/tasks/{id}/replay-chain` | GET | P5 | `ReplayChainQueryService` |
| `/ops/v1/tasks/{id}/recovery-explanation` | GET | P4 | `RecoveryExplainerService` |
| `/ops/v1/tasks/{id}/replay` | POST | P3/P4 | `ReplayLineageService` + `ReplayPolicyService` |
| `/ops/v1/tasks/{id}/force-lease-eviction` | POST | P5 | `ForceOperationService` |
| `/ops/v1/dags/{id}/runs` | GET | P4 | `RunCentricQueryService` |
| `/ops/v1/operator-actions` | GET | P3 | `OperatorQueryService` |

---

## 4. Code Review Findings

### ✅ No critical issues found

### Issues Fixed During This Review

| # | File | Finding | Fix |
|---|---|---|---|
| 1 | `src/models/task_run.py` | Missing `attempt`, `worker_id`, `lease_id` fields; `run_tracking.py` failed at runtime | Added fields |
| 2 | `docs/plans/p5-platform-productization-plan.md` | Was a copy of P4 plan (content mismatch) | Replaced with P5-specific completed plan |
| 3 | `docs/plans/p3/p4-plan.md` | Status remained "Draft" despite completion | Updated to "Completed" |

### Minor code smells (non-blocking)

| # | Location | Observation | Severity |
|---|---|---|---|
| 1 | All service files | `_maybe_await` duplicated in 16 files | Low — extract to `src/common/db_utils.py` in future |
| 2 | `callback_ops.py`, `operator_actions.py` | `except Exception: pass` in audit paths | Acceptable — best-effort audit should not fail primary op |
| 3 | `recovery_explainer.py` | `TaskRecord` import is inside try/except; model field names assumed | Low — could tighten coupling to actual model |
| 4 | `force_operations.py` | Force lease eviction endpoint is wired but actual Redis key deletion is a stub | Expected — comment in code documents this |
| 5 | `src/services/governance.py` | High-risk ops set is a hardcoded frozenset-equivalent | Acceptable for current phase; future: load from config |

---

## 5. Test Coverage Summary

| Test file | Covers |
|---|---|
| `test_task_state_machine.py` | Task FSM transitions |
| `test_callback_outbox.py` | Outbox model and dispatch |
| `test_task_completion_node.py` | Completion → outbox write |
| `test_run_tracking.py` | TaskRun/DagRun creation |
| `test_task_timeline.py` | Timeline event emission |
| `test_callback_ops.py` | Dead-letter list/ack/replay |
| `test_task_audit_queries.py` | Timeline/callback summary queries |
| `test_ops_audit_endpoints.py` | Ops router coverage |
| `test_operator_actions.py` | Operator action persistence |
| `test_operator_queries.py` | Operator action list/filter |
| `test_replay_lineage.py` | Task replay + lineage creation |
| `test_operator_dashboard.py` | Dashboard summary aggregation |
| `test_recovery_explainer.py` | Stale task explanation |
| `test_replay_policy.py` | Task replay policy checks |
| `test_run_centric_queries.py` | Task/dag run queries |
| `test_governance.py` | Role/risk classification |
| `test_operator_ux.py` | Operator UX queues |
| `test_callback_replay_policy.py` | Callback replay policy |
| `test_force_operations.py` | Force operation governance |
| `test_replay_chain_queries.py` | Replay chain query |

Total: **519 tests passing**

---

## 6. Architecture Consistency Check

### ✅ Single truth source (MySQL)
All new models write to MySQL. Redis used only for lease checks and cache.

### ✅ Operator audit completeness
All human interventions (replay, ack, force ops) go through `OperatorActionService` → `operator_actions` table.

### ✅ Timeline coverage
System events and operator events both emit to `task_events` table via `TaskTimelineService`.

### ✅ Policy enforcement
- Task replay: `ReplayPolicyService` (reason + lease check + role)
- Callback replay: `CallbackReplayPolicyService` (reason + status check + role)
- Force ops: `ForceOperationService` (role + reason via `GovernanceService`)

### ✅ Lineage traceability
- `TaskRun` records created for every replay
- `from_run_key` / `new_run_key` tracked in operator action payload
- `replay-chain` endpoint exposes full lineage ordered by time

---

## 7. Recommended Next Actions (P6 candidates)

| Priority | Item |
|---|---|
| Medium | Extract `_maybe_await` to `src/common/db_utils.py` |
| Medium | Implement actual Redis lock eviction in `force-lease-eviction` endpoint |
| Low | Add replay frequency rate limiting per task |
| Low | Add `GovernanceService` config loading (instead of hardcoded set) |
| Low | Add `recovery_explainer` tight coupling to actual `TaskRecord` model fields |
