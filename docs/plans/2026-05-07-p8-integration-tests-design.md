# P8 End-to-End Integration Tests Design

Date: 2026-05-07
Status: Draft
Depends on:
- `docs/plans/2026-05-07-p7-observability-design.md`

---

## 1. Background

P0–P7 established complete platform capabilities with 532 unit tests. However, all tests use mocked DB sessions and fake Redis. There are no integration tests that verify the full path end-to-end with real storage.

P8 adds integration test coverage for the three most critical platform paths:
1. Force-lease-eviction with real Redis key deletion
2. Replay lineage chain with real DB writes
3. Dead-letter callback replay path end-to-end

---

## 2. P8 Goals

### Goal A — Integration test infrastructure
- SQLite in-memory async DB for integration tests (no external MySQL needed)
- FullFakeAsyncRedis already exists — wire it as the Redis backend
- Shared async session factory fixture

### Goal B — Replay lineage integration scenario
- Create a TaskRun via RunTrackingService
- Request replay via ReplayLineageService
- Verify new TaskRun is created with correct lineage fields
- Verify operator action is recorded

### Goal C — Force-lease-eviction integration scenario
- Set a Redis lock key
- Call force-lease-eviction via ForceOperationService with executor
- Verify Redis key is deleted
- Verify operator action is recorded

### Goal D — Dead-letter callback replay integration scenario
- Write a CallbackOutboxRecord with DEAD_LETTER status
- Call replay_dead_letter via CallbackOpsService
- Verify status reset to PENDING
- Verify operator action recorded

---

## 3. Non-goals

- External MySQL/Redis process (use in-memory/fake)
- Full API-level HTTP integration tests
- Load / performance tests

---

## 4. Acceptance Criteria

P8 is complete when:
- Integration test fixtures (async SQLite session, fake Redis) are established
- 3 integration scenarios pass (replay lineage, force eviction, dead-letter replay)
- All 532+ existing tests still pass
