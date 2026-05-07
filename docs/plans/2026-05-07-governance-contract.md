# Governance Contract

Date: 2026-05-07
Status: Active

---

## 1. Intervention role categories

| Role | Description |
|---|---|
| `operator` | Standard ops personnel. Can replay, ack, view, query. |
| `admin` | Elevated. Required for high-risk force operations. |

---

## 2. High-risk operations

These operations require the `admin` role and a non-empty reason:

| Operation | Risk |
|---|---|
| `force_replay_on_active_lease` | May conflict with live execution |
| `force_replay_on_running_task` | May cause duplicate runs |
| `force_replay_without_reason` | No audit trail |
| `force_acknowledge_dead_letter_without_review` | Skips inspection |
| `force_lease_eviction` | Hard eviction; may leave orphaned workers |

---

## 3. Audit requirements

| Operation type | Minimum audit requirement |
|---|---|
| Standard operator action | reason optional, actor logged |
| High-risk force operation | reason required, actor role logged |
| Callback replay | reason required |
| Task replay | reason required, lease state checked |

---

## 4. Policy enforcement points

| Endpoint | Policy check | Service |
|---|---|---|
| `POST /ops/v1/tasks/{task_id}/replay` | lease check, reason, role | `ReplayPolicyService` |
| `POST /ops/v1/callbacks/dead-letters/{id}/replay` | status check, reason, role | `CallbackReplayPolicyService` |
| `POST /ops/v1/tasks/{task_id}/force-lease-eviction` | admin role, reason required | `ForceOperationService` |

---

## 5. Audit trail coverage

All operator interventions are durably recorded in `operator_actions` table via `OperatorActionService`.
High-risk force operations additionally emit a task timeline event.

---

## 6. Limitations (current)

- No external IAM integration (role comes from auth token claim)
- No approval/two-person-review workflow
- No rate limiting on replay frequency per task
- Force operations do not yet perform actual Redis lease eviction (hook point exists)
