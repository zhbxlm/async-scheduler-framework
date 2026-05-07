# P6 Technical Debt & Hardening Design

Date: 2026-05-07
Status: Draft
Depends on:
- `docs/plans/2026-05-07-p0-p5-alignment-review.md`

---

## 1. Background

P0–P5 established a complete platform skeleton covering control-plane, lifecycle, audit, replay, operator experience, and governance. The alignment review identified a set of technical debt items and hardening opportunities that should be addressed before the platform can be considered production-ready.

P6 addresses these specifically:

1. DRY: extract `_maybe_await` helper from 16 service files into `src/common/db_utils.py`
2. Force lease eviction: implement actual Redis lock deletion in the force-lease-eviction path
3. Replay rate limiting: prevent replay storms per task
4. Governance configuration: load high-risk operation definitions from config, not hardcode
5. RecoveryExplainer model binding: tighten coupling to actual `TaskRecord` fields

---

## 2. P6 Goals

### Goal A — DRY: common async db helpers
Remove duplicated `_maybe_await` from all service files. Centralise in `src/common/db_utils.py`.

### Goal B — Real force-lease-eviction
The `force-lease-eviction` endpoint exists but does not actually delete the Redis lock key. Wire in real Redis key deletion with confirmation.

### Goal C — Replay rate limiting
Add per-task replay frequency guard. Deny replay if too many replays have been requested for the same task within a sliding window.

### Goal D — Governance config loading
Move the high-risk operations set from `GovernanceService` hardcode to a config-driven structure. Allow environment-level overrides.

### Goal E — RecoveryExplainer model binding
Remove field-name assumptions in `RecoveryExplainerService`. Reference actual `TaskRecord` columns.

---

## 3. Non-goals

- New product features
- External IAM integration
- UI / dashboard frontend

---

## 4. Design Details

### 4.1 Common db_utils

```python
# src/common/db_utils.py
import inspect

async def maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value
```

All service files import `from src.common.db_utils import maybe_await` and remove their local copy.

---

### 4.2 Real force-lease-eviction

The `ForceOperationService.execute_force_operation` accepts an optional `executor` callable for the actual side-effect. The ops endpoint passes a Redis key deletion callable:

```python
async def _evict_lease(task_id, redis):
    key = f"task_lock:{task_id}"
    await redis.delete(key)
```

This keeps `ForceOperationService` policy-only and testable without Redis.

---

### 4.3 Replay rate limiting

`ReplayPolicyService.check_task_replay_allowed` checks `operator_actions` table:
- count replays for `task_id` within last `window_seconds` (default 300s)
- deny if count >= `max_replays` (default 3)

This uses the existing `operator_actions` table — no new model needed.

---

### 4.4 Governance config

```python
class GovernanceService:
    DEFAULT_HIGH_RISK_OPS = frozenset({...})

    def __init__(self, *, config: dict | None = None, session_factory=None):
        self._high_risk = frozenset(config.get("high_risk_operations", [])) if config else self.DEFAULT_HIGH_RISK_OPS
```

Caller can pass a config dict; default behaviour is unchanged.

---

### 4.5 RecoveryExplainer model binding

Import `TaskRecord` and reference `TaskRecord.status`, `TaskRecord.attempt`, `TaskRecord.max_retries` directly. Remove `getattr` fallbacks for known columns.

---

## 5. Acceptance Criteria

P6 is complete when:
- `_maybe_await` exists only in `src/common/db_utils.py`
- force-lease-eviction deletes the Redis key and returns confirmation
- replay rate limiting prevents >3 replays within 5 minutes
- `GovernanceService` accepts optional config override
- `RecoveryExplainerService` references actual `TaskRecord` fields directly
- All 519+ tests still pass
