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

---

## Addendum — latest round summary (18:00 update)

### Final review summary (EN)

This branch continues pushing the async scheduler framework toward a **real Redis-backed distributed kernel**, and complements that work with **converged recovery semantics, lease observability, fault-injection coverage, and refreshed README documentation**.

#### What was added in this round

##### 1. Lease observability / debugging surface
A new lease- and heartbeat-focused debugging surface was added:

- `GET /debug/leases/{task_id}`
- `GET /debug/leases`
- `GET /workers/{worker_id}/leases`

`/debug/summary` was also extended with lease-related aggregate signals:

- `locked_count`
- `running_with_lock_count`
- `running_without_lock_count`
- `locked_but_terminal_count`
- `abandoned_but_running_count`
- `stale_lease_count`

`/debug/leases` now supports filtering by:
- `worker_id`
- `task_status`
- `attempt_status`
- `locked_only`

##### 2. High-value fault-injection coverage around finalize / callback / reconciler overlap
This round also adds recovery-boundary tests that pin down convergence semantics under dirty distributed edges:

- **duplicate finalize overlap + callback failure**
- **concurrent reconcile overlap**
- **callback failure + lease loss + reconciler overlap**

These tests further clarify the system’s intended semantics:
- **at-least-once side effects**
- **terminal-state-first persistence**
- **eventual convergence**

##### 3. README refresh
The README was updated to reflect the current runtime reality more accurately, including:

- the presence of a **real Redis-backed distributed kernel path**
- real Redis-backed queue / lock / completion dedupe / worker registry paths
- converged retry exhaustion semantics
- current live Redis validation flow
- new observability endpoints and debugging workflow
- updated deepwiki-alignment boundaries
- refreshed roadmap and current-status sections

### Key recent commits
- `9805d53` — `feat: add lease observability debug endpoints`
- `e19cf69` — `test: cover finalize and reconciler overlap recovery boundaries`
- `87ae6f2` — `docs: refresh README for current distributed runtime state`
- `ba69613` — `docs: sync final PR body into handoff notes`

### Additional follow-up now completed
- added `GET /debug/leases/anomalies` for anomaly-oriented lease/debug inspection
- added `anomaly_summary` to `/debug/summary`
- added `docs/runtime/distributed-deployment-guide.md` (now with single-host multi-process and small multi-node examples)
- added `docs/reference/non-critical-shared-state-boundary.md`
- added `docs/reference/next-phase-gap-analysis.md`
- extended P2 control-point failure coverage across heartbeat, completion dedupe, reconciler liveness lookup, and requeue enqueue paths
- refreshed deepwiki reference to reflect completed observability/fault-injection progress

### Recent regression pass
Executed:

```bash
pytest -q \
  tests/test_observability_api.py \
  tests/integration/test_failure_recovery.py \
  tests/integration/test_live_redis_retry_exhaustion.py
```

Result:
- **16 passed**
- **1 skipped** (expected live-Redis conditional skip when environment requirements are not present)

### Suggested reviewer reading order
1. `async_scheduler/api/app.py`
2. `async_scheduler/platform/services.py`
3. `async_scheduler/backends/redis.py`
4. `async_scheduler/persistence/repositories.py`
5. `tests/test_observability_api.py`
6. `tests/integration/test_failure_recovery.py`
7. `README.md`

---

### 超短版提审说明（中文）

这轮主要补了三块：

- **lease observability**
  - 新增 `/debug/leases`、`/debug/leases/{task_id}`、`/workers/{worker_id}/leases`
  - `/debug/summary` 补了 lease 相关异常统计

- **fault injection / recovery boundary**
  - 覆盖 finalize overlap、callback failure、concurrent reconcile、lease loss + reconciler overlap 等关键脏边界
  - 进一步钉住系统的 **terminal-state-first + eventual convergence** 语义

- **README refresh**
  - 把当前 real Redis 路径、observability、验证方式和 deepwiki 对齐边界同步到了文档里

最近相关提交：
- `9805d53`
- `e19cf69`
- `87ae6f2`

最近回归：
- `16 passed, 1 skipped`

---

### Ultra-short review note (EN)

This round mainly adds:

- **lease observability**
  - `/debug/leases`
  - `/debug/leases/{task_id}`
  - `/workers/{worker_id}/leases`
  - richer lease anomaly summary in `/debug/summary`

- **fault-injection recovery coverage**
  - finalize overlap
  - callback failure
  - concurrent reconciler overlap
  - lease-loss + reconciler-overlap boundary

- **README refresh**
  - updated to reflect the current real Redis-backed runtime state and debugging surface

Recent commits:
- `9805d53`
- `e19cf69`
- `87ae6f2`

Recent regression:
- **16 passed, 1 skipped**

### Latest addition (final push)
This final push adds:

- **multi-control-point partition-like simulation**
  - Sequential failure across lock, dedupe, and registry control points
  - Validates best-effort continuation and recovery

- **delayed promotion under concurrent load**
  - 10 tasks with identical scheduled time
  - Verifies concurrent promotion and consumption semantics
  - Ensures no task loss or duplication

- **DAG branch semantics refinement**
  - Partial branch success with downstream cancellation vs. failure interplay
  - Fan-out/fan-in with failure propagation
  - Long-running branch cancellation + sibling success preservation

Latest regression:
- **14 passed** in failure recovery suite
- **14 passed** in DAG engine suite

---

## Final PR body (CN)

### 标题
`feat: 对齐分布式调度器的 real Redis 验证、lease 可观测性与恢复语义收敛`

### 概述
这个 PR 继续把当前调度框架往 **real Redis-backed distributed kernel** 推进，并同时补强了：恢复语义收敛、lease / heartbeat 可观测性、高价值 fault-injection 验证，以及 README / 提审材料的状态同步。

### 本次改动内容

#### 1. lease observability / 调试面补齐
新增：
- `GET /debug/leases/{task_id}`
- `GET /debug/leases`
- `GET /workers/{worker_id}/leases`

增强 `/debug/summary`：
- `locked_count`
- `running_with_lock_count`
- `running_without_lock_count`
- `locked_but_terminal_count`
- `abandoned_but_running_count`
- `stale_lease_count`

`/debug/leases` 支持按以下维度过滤：
- `worker_id`
- `task_status`
- `attempt_status`
- `locked_only`

#### 2. 恢复边界 fault-injection 测试补强
新增覆盖：
- **duplicate finalize overlap + callback failure**
- **concurrent reconcile overlap**
- **callback failure + lease loss + reconciler overlap**

系统语义进一步明确为：
- **at-least-once side effects**
- **terminal-state-first persistence**
- **eventual convergence**

#### 3. README 刷新
README 已更新：
- real Redis-backed distributed kernel path 的当前状态
- live Redis 验证方式
- observability 端点与排障流程
- deepwiki 对齐边界
- 路线图与当前状态总结

### 关键语义
- 普通失败且重试预算耗尽：`FAILED / FAILED`
- lease loss：`FAILED / ABANDONED`
- 终态持久化后：callback failure 不回滚终态，reconciler 不误回队

### 最近关键提交
- `9805d53` — `feat: add lease observability debug endpoints`
- `e19cf69` — `test: cover finalize and reconciler overlap recovery boundaries`
- `87ae6f2` — `docs: refresh README for current distributed runtime state`
- `1fe6f85` — `docs: update PR handoff notes with observability addendum`
- `ba69613` — `docs: sync final PR body into handoff notes`

### 本轮后续补充（已完成）
- 新增 `GET /debug/leases/anomalies`，用于 anomaly-oriented lease / recovery 排障
- 在 `/debug/summary` 中补入 `anomaly_summary`
- 新增 `docs/runtime/distributed-deployment-guide.md`（含单机多进程 / 小规模多节点示例）
- 新增 `docs/reference/non-critical-shared-state-boundary.md`
- 新增 `docs/reference/next-phase-gap-analysis.md`
- 扩展 P2 control-point failure 覆盖：heartbeat / completion dedupe / reconciler liveness lookup / requeue enqueue
- 更新 deepwiki distributed reference，重排当前剩余 gap 与 next iteration

### 定向回归结果
```bash
pytest -q \
  tests/test_observability_api.py \
  tests/integration/test_failure_recovery.py \
  tests/integration/test_live_redis_retry_exhaustion.py
```

结果：
- **16 passed**
- **1 skipped**

### 建议 reviewer 阅读顺序
1. `async_scheduler/api/app.py`
2. `async_scheduler/platform/services.py`
3. `async_scheduler/backends/redis.py`
4. `async_scheduler/persistence/repositories.py`
5. `tests/test_observability_api.py`
6. `tests/integration/test_failure_recovery.py`
7. `README.md`
