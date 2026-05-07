# Real Redis Integration Validation Plan

> Historical implementation plan. This document reflects an earlier design/verification phase and may reference modules or naming that no longer match the current repository layout.

Date: 2026-05-01
Project: ray-async-framework
Status: Partially completed

## Goal
Validate that the newly added real-Redis client paths for queue, lock, worker registry,
and completion dedupe can work together as one distributed coordination chain.

## Constraints
- Keep memory mode working
- Preserve TDD
- Do not claim exactly-once
- Prefer deterministic fake-client integration before requiring a live Redis server

## Tasks

### Task 1 — Add shared fake Redis integration harness
- [x] create one fake async Redis client that supports the subset used by:
  - RedisQueueBackend
  - RedisLockBackend
  - RedisCompletionDedupBackend
  - WorkerRegistry
- [x] tests first
- [x] verify targeted integration tests pass

### Task 2 — Add end-to-end coordination integration test
- [x] register worker
- [x] enqueue task
- [x] acquire lease
- [x] complete once via dedupe backend
- [x] verify duplicate completion is rejected
- [x] verify worker remains live during flow

### Task 3 — Add lease-loss / recovery integration test on shared client
- [x] worker A acquires
- [x] worker B cannot acquire while lease valid
- [x] release/expiry path allows re-acquire
- [x] recovery-oriented invariants added for duplicate completion / token ownership / queue cleanup

### Task 4 — Update docs and smoke notes for real Redis transition state
- [x] document current state as partial real Redis-backed transition
- [x] document how to plug a real async Redis client
- [x] document Redis transition integration test entrypoints and fakeredis skip behavior
- [x] add dedicated distributed smoke variant for shared fake-client path with optional fakeredis compatibility check
- [x] add dedicated distributed smoke variant against a live Redis-compatible backend

## Verification
- `pytest -q tests/integration/test_real_redis_coordination.py`
- `pytest -q tests/integration/test_real_redis_coordination_more.py`
- `pytest -q tests/integration/test_real_redis_recovery_invariants.py`
- `python3 scripts/distributed_smoke_test.py`
- `pytest -q` → 101 passed
