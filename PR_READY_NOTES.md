# PR Ready Notes

Branch: `feat/deepwiki-distributed-alignment`
Prepared at: 2026-05-02 16:41 Asia/Singapore

## Suggested PR title

`feat: align distributed scheduler with real Redis verification and recovery semantics`

## Short PR summary

This PR advances the distributed scheduler toward stronger real-Redis-backed coordination and clearer distributed recovery semantics.

### Highlights
- moves critical queue/lock coordination paths closer to real Redis-backed behavior
- converges retry exhaustion semantics across executor, consumer, and worker paths
- strengthens task/attempt terminal-state consistency
- adds fault-injection coverage for lease loss, callback-dispatch overlap, and partial-work recovery
- expands the live Redis verification suite and observability coverage
- updates distributed architecture/reference docs to match current implementation state

### Key convergence rules after this PR
- ordinary retry-budget exhaustion:
  - `TaskStatus.FAILED`
  - `ExecutionAttemptStatus.FAILED`
- lease loss during execution:
  - `TaskStatus.FAILED`
  - `ExecutionAttemptStatus.ABANDONED`

### Notes
The system target remains **at-least-once with convergence**, not strict exactly-once.

---

## Chinese PR summary

这个 PR 主要把当前调度框架的分布式内核，往更接近 deepwiki 参考架构的方向推进，重点包括：

- 关键 queue / lock 路径进一步向 real Redis 共享状态语义收敛
- executor / consumer / worker 的重试耗尽语义对齐
- task / execution attempt 终态一致性加强
- 补强 lease loss、callback overlap、partial work recovery 等关键故障边界测试
- 扩展 live Redis 验证套件与 observability 测试
- 同步更新 deepwiki 对齐文档与阶段性总结

核心语义：
- 普通失败且重试预算耗尽 → `FAILED / FAILED`
- lease loss → `FAILED / ABANDONED`
- partial work recovery 采用 **at-least-once + convergence**，不承诺 exactly-once

---

## Reviewer guide

### Recommended review order
1. `async_scheduler/backends/redis.py`
2. `async_scheduler/core/consumer.py`
3. `async_scheduler/executor/executor.py`
4. `async_scheduler/worker/base.py`
5. `async_scheduler/platform/completion.py`
6. `async_scheduler/platform/reconciler.py`
7. `tests/integration/test_failure_recovery.py`
8. `tests/test_consumer_attempt_consistency.py`
9. `tests/test_task_worker_retry_semantics.py`
10. `tests/integration/test_live_redis_*`

### What reviewers should validate
- retry budget should be consumed inside `TaskExecutor.execute()`
- consumer/worker should not requeue again after retry exhaustion
- exhausted ordinary failure should converge to `FAILED / FAILED`
- lease loss should converge to `FAILED / ABANDONED`
- callback-dispatch lease-loss should not requeue already-terminal tasks
- partial-work + recovery behavior should match at-least-once + convergence semantics
- real Redis critical paths should be materially stronger than prior Redis-shaped fallback behavior

---

## Short review-note / group-message version (CN)

这轮主要把分布式调度内核往 **real Redis + 更清晰恢复语义** 推进了一步，核心包括：

- 收敛 executor / consumer / worker 的重试耗尽语义
- 普通失败统一到 `FAILED / FAILED`，lease loss 保持 `FAILED / ABANDONED`
- 补强 queue / lock critical path 的 real Redis 验证
- 增加多组故障注入与 live Redis integration tests
- 覆盖 callback dispatch 期间 lease 丢失、partial work crash recovery 等关键边界
- 同步补了 observability 测试和 deepwiki 对齐文档

建议 reviewer 先看：
`redis.py` → `consumer.py` → `executor.py` → `reconciler.py` → `test_failure_recovery.py`

---

## Short review-note / group-message version (EN)

This branch pushes the distributed scheduler toward **real Redis-backed coordination and clearer recovery semantics**.

Highlights:
- converges retry-exhaustion behavior across executor / consumer / worker paths
- ordinary exhausted failure now converges to `FAILED / FAILED`
- lease loss remains `FAILED / ABANDONED`
- strengthens real Redis verification for queue / lock critical paths
- adds fault-injection and live Redis integration coverage
- covers callback-dispatch lease-loss and partial-work recovery boundaries
- updates observability tests and deepwiki-aligned docs

Recommended review path:
`redis.py` → `consumer.py` → `executor.py` → `reconciler.py` → `test_failure_recovery.py`

---

## Suggested commit grouping for review

### Group 1 — Kernel semantics
- `81fefc8`
- `cf9f7b2`
- `d214988`

### Group 2 — Recovery / fault injection
- `0b5cffa`
- `5bac6ac`

### Group 3 — Verification / observability / docs
- `2841ff5`

---

## Final pre-review checklist
- ensure working tree is clean: `git status --short`
- include at least one targeted test result block in PR description
- explicitly call out semantic changes, not just "more tests"
- if reviewers prefer smaller history, squash by the 3 groups above rather than flattening everything into one commit
