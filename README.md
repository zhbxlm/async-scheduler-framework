# Async Scheduler Framework

一个本地可运行、并已经演进到 **可验证的分布式调度内核骨架** 的异步调度框架，参考 ray-amu deepwiki 的能力边界，提供：

> 当前仓库已经具备一条可工作的 **real Redis-backed distributed kernel path**：包括 queue、lease / heartbeat、worker registry、execution attempts、completion idempotency、distributed reconciler，以及 executor / consumer / worker 之间收敛后的 retry exhaustion 语义。关键共享状态路径（queue / lock / completion dedupe / worker registry）已经支持真实 async Redis client；queue promotion 与 lock compare-and-act 等关键操作已补入 Lua/CAS 风格原子语义；同时保留无真实 Redis client 时的进程内 fallback 模式。仓库还提供 live Redis smoke / recovery / overlap / consumer-recovery / retry-exhaustion 验证套件，以及一组面向 finalize / callback / reconciler overlap 的高价值 fault-injection 测试。它仍然不是最终形态的生产级 deepwiki 等价实现，但已经具备真正多进程 / 多节点部署所需的核心语义与验证基础。

- FastAPI 任务 API
- SQLite 持久化
- 优先级队列与延时任务
- 任务消费循环
- 带超时 / 重试 / 取消的执行器
- DAG 编排引擎（依赖、并行、条件、skip/fallback 基础能力）
- Cron 定时调度
- Worker 抽象和示例 worker
- CLI 启动入口
- Capability Registry
- Tenant / Quota 基础治理
- Reconciler 后台修复

## 项目结构

```text
async_scheduler/
  api/           # FastAPI 应用
  cli/           # CLI 入口
  backends/      # Backend 抽象层（支持内存/Redis 等可插拔存储）🆕
  core/          # 核心模型与 consumer
  dag/           # DAG 引擎 / loader / step executors
  executor/      # 任务执行器
  persistence/   # SQLAlchemy + repository
  platform/      # router / quota / completion / reconciler / handlers
  queue/         # 优先级队列（Backend 抽象）
  registry/      # capability registry
  scheduler/     # cron scheduler + schedule registry（Backend 抽象）
  worker/        # worker 抽象与示例
tests/           # pytest 测试
examples/        # demo 文档
```

## 快速开始

### 1. 安装依赖

```bash
cd /home/gem/.openclaw/workspace/projects/async-scheduler-framework
python3 -m pip install -e .[dev] --no-build-isolation
```

### 2. 初始化数据库

```bash
async-scheduler init-db-cmd --force
```

### 3. 启动 API

```bash
async-scheduler api --init-db
```

打开：
- http://127.0.0.1:8000/docs
- http://127.0.0.1:8000/health

### 4. 启动开发模式

```bash
async-scheduler dev --init-db
```

### 5. 运行测试

```bash
pytest -q
```

### 6. 运行烟雾测试

烟雾测试用于验证框架的核心功能是否正常工作：

```bash
python -m scripts.smoke_test
```

> 当前 `scripts/smoke_test.py` 主要覆盖内存模式和基础框架可用性；涉及真实 Redis 共享状态语义的验证请使用下面的 integration tests。

### 7. 运行 Redis 集成测试

当前仓库支持两类 Redis 相关验证：

#### 7.1 shared-client / fallback 集成测试

```bash
pytest -q tests/integration/test_real_redis_coordination.py
pytest -q tests/integration/test_real_redis_coordination_more.py
pytest -q tests/integration/test_real_redis_recovery_invariants.py
```

如果环境里安装了 `fakeredis` 且其 `fakeredis.aioredis` 可用，还可以运行：

```bash
pytest -q tests/integration/test_fakeredis_coordination.py
```

若 `fakeredis.aioredis` 不可用，该测试会自动 skip，这是预期行为。

#### 7.2 live Redis 验证套件（推荐）

当你有真实 Redis / Redis-compatible 环境时，优先跑下面这组：

```bash
TEST_REDIS_URL=redis://localhost:6379/0 python3 scripts/live_redis_smoke_test.py
TEST_REDIS_URL=redis://localhost:6379/0 python3 scripts/live_redis_suite.py                 # all
TEST_REDIS_URL=redis://localhost:6379/0 python3 scripts/live_redis_suite.py smoke           # smoke only
TEST_REDIS_URL=redis://localhost:6379/0 python3 scripts/live_redis_suite.py recovery        # recovery only
TEST_REDIS_URL=redis://localhost:6379/0 python3 scripts/live_redis_suite.py overlap         # overlap/race only
TEST_REDIS_URL=redis://localhost:6379/0 python3 scripts/live_redis_suite.py overlap --fail-fast
```

也可以按文件单跑：

```bash
pytest -q tests/integration/test_live_redis_smoke.py
pytest -q tests/integration/test_live_redis_recovery.py
pytest -q tests/integration/test_live_redis_completion_overlap.py
pytest -q tests/integration/test_live_redis_duplicate_completion_overlap.py
pytest -q tests/integration/test_live_redis_lease_loss_completion_race.py
pytest -q tests/integration/test_live_redis_attempt_consistency_overlap.py
pytest -q tests/integration/test_live_redis_multi_worker_overlap.py
pytest -q tests/integration/test_live_redis_multi_worker_delayed_promotion.py
pytest -q tests/integration/test_live_redis_multi_worker_dead_owner_recovery.py
pytest -q tests/integration/test_live_redis_end_to_end_consumer_loop.py
pytest -q tests/integration/test_live_redis_consumer_recovery.py
pytest -q tests/integration/test_live_redis_retry_exhaustion.py
```

说明：
- 上述 `test_live_redis_*` 用例仅在设置 `TEST_REDIS_URL` 时运行
- 未设置环境变量时会自动 skip，不影响默认本地回归

### 8. 运行 distributed smoke test

仓库还提供了一个 dedicated distributed smoke variant：

```bash
python3 scripts/distributed_smoke_test.py
```

它会：
- 默认跑 shared fake async Redis client 的协调链 smoke
- 如果环境中可用 `fakeredis.aioredis`，再追加跑一层 fakeredis compatibility smoke
- 在缺少 fakeredis 模块时以 skip 方式降级，而不是报错失败

如果你已经准备了可访问的真实 Redis / Redis-compatible 环境，还可以执行：

```bash
TEST_REDIS_URL=redis://localhost:6379/0 python3 scripts/live_redis_smoke_test.py
TEST_REDIS_URL=redis://localhost:6379/0 python3 scripts/live_redis_suite.py                 # all
TEST_REDIS_URL=redis://localhost:6379/0 python3 scripts/live_redis_suite.py smoke           # smoke only
TEST_REDIS_URL=redis://localhost:6379/0 python3 scripts/live_redis_suite.py recovery        # recovery only
TEST_REDIS_URL=redis://localhost:6379/0 python3 scripts/live_redis_suite.py overlap         # overlap/race only
TEST_REDIS_URL=redis://localhost:6379/0 python3 scripts/live_redis_suite.py overlap --fail-fast

# or run individual live Redis segments
pytest -q tests/integration/test_live_redis_smoke.py
pytest -q tests/integration/test_live_redis_recovery.py
pytest -q tests/integration/test_live_redis_completion_overlap.py
pytest -q tests/integration/test_live_redis_duplicate_completion_overlap.py
pytest -q tests/integration/test_live_redis_lease_loss_completion_race.py
pytest -q tests/integration/test_live_redis_attempt_consistency_overlap.py
pytest -q tests/integration/test_live_redis_multi_worker_overlap.py
pytest -q tests/integration/test_live_redis_multi_worker_delayed_promotion.py
pytest -q tests/integration/test_live_redis_multi_worker_dead_owner_recovery.py
pytest -q tests/integration/test_live_redis_end_to_end_consumer_loop.py
pytest -q tests/integration/test_live_redis_consumer_recovery.py
```

说明：
- `tests/integration/test_live_redis_smoke.py` 仅在设置 `TEST_REDIS_URL` 时运行
- 未设置环境变量时会自动 skip，不影响默认本地回归

## 常用 CLI

### 创建任务

```bash
async-scheduler task demo --payload '{"capability":"echo","message":"hello"}'
```

### 创建调度

```bash
async-scheduler schedule heartbeat '*/5 * * * *' --payload '{"capability":"echo","message":"tick"}'
```

### 查看状态

```bash
async-scheduler status
```

### 手动跑一次 reconciler

```bash
async-scheduler reconcile
```

## 常用 API

### 健康检查

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/queue/stats
```

说明：
- `/health` 提供基础运行状态
- `/queue/stats` 提供队列大小、调度数量与执行中任务统计
- 更完整的恢复 / lease / worker 诊断请看下面的 observability 端点

### 创建租户

```bash
curl -X POST http://127.0.0.1:8000/tenants \
  -H 'Content-Type: application/json' \
  -d '{"name":"team-a","config":{"max_queued":20,"max_running":5}}'
```

### 创建任务

```bash
curl -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"compute-task",
    "tenant_id":"tenant-a",
    "idempotency_key":"demo-task-1",
    "payload":{"capability":"compute","operation":"add","a":1,"b":2}
  }'
```

### 查看能力列表

```bash
curl http://127.0.0.1:8000/capabilities
```

### 创建 Schedule

```bash
curl -X POST http://127.0.0.1:8000/schedules \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"heartbeat",
    "cron_expression":"*/5 * * * *",
    "dedup_window_seconds":60,
    "task_template":{"capability":"echo","message":"tick"}
  }'
```

### Pause / Resume Schedule

```bash
curl -X POST http://127.0.0.1:8000/schedules/<schedule_id>/pause
curl -X POST http://127.0.0.1:8000/schedules/<schedule_id>/resume
```

### 手动运行 Reconciler

```bash
curl -X POST http://127.0.0.1:8000/reconciler/run
```

### 查看 Attempt / Worker / Lease / Reconciler 诊断信息

```bash
curl http://127.0.0.1:8000/tasks/<task_id>/attempts
curl http://127.0.0.1:8000/tasks/<task_id>/attempts/latest
curl http://127.0.0.1:8000/workers
curl http://127.0.0.1:8000/workers/<worker_id>
curl http://127.0.0.1:8000/workers/<worker_id>/leases
curl http://127.0.0.1:8000/reconciler/history
curl 'http://127.0.0.1:8000/reconciler/history?action=requeue'
curl 'http://127.0.0.1:8000/reconciler/history?task_id=<task_id>'
curl http://127.0.0.1:8000/debug/summary
curl http://127.0.0.1:8000/debug/leases
curl http://127.0.0.1:8000/debug/leases/anomalies
curl 'http://127.0.0.1:8000/debug/leases?task_status=running&locked_only=true'
curl 'http://127.0.0.1:8000/debug/leases?worker_id=<worker_id>'
curl http://127.0.0.1:8000/debug/leases/<task_id>
```

推荐排障顺序：
- 先看 `/debug/summary`，快速判断是否存在 `running_without_lock_count`、`locked_but_terminal_count`、`abandoned_but_running_count` 这类异常计数
- 再看 `/debug/leases/anomalies`，直接定位 suspicious states
- 再用 `/debug/leases` 按 `worker_id` / `task_status` / `attempt_status` / `locked_only` 过滤可疑任务
- 最后用 `/debug/leases/<task_id>` 看单任务 lease / latest attempt 详情

## 端到端 Demo

见：

- `examples/end_to_end_demo.md`

## 与 deepwiki 的对齐边界

这个仓库当前应被视为：

- **ray-amu / deepwiki 设计启发下的单机版 MVP**
- **本地可运行、可测试、可演进的结构原型**
- **不是 deepwiki 分布式平台的等价实现**

### 已对齐的方向

- TaskRouter / Queue / Consumer / Executor / DAGEngine 主链
- 多租户 / quota 的基础治理语义
- task idempotency 与 schedule dedup 的平台语义
- capability registry 与 DAG loader 的基本分层
- completion node / schedule registry / reconciler / step executors 的结构存在
- CLI + API + persistence 的工程骨架

### 尚未完全对齐的部分

- 还没有 Ray / SchedulerActor / ActorPoolManager
- 还没有 ResourceManager / NodeAgent / Cluster 管理
- 还没有 Async Proxy sidecar 的真实实现
- DAG 模型仍是简化版，尚未完整覆盖 step_kind / map / streaming / flask_wrapped 全语义
- quota 仍是本地内存实现，不是 deepwiki 的 Redis 原子配额执行器
- callback / side-effect delivery 仍是轻量实现，尚未扩展到真实外部交付保证链路
- 仍缺更完整的生产级 live Redis 矩阵、压测、告警与运行时硬化

## 当前状态

这是一个 **已经具备真实 Redis 关键共享状态路径、恢复语义收敛、以及故障注入验证的分布式调度内核骨架仓库**，适合继续做二次开发，并进一步向 deepwiki 风格的生产级分布式平台收敛。

### 当前已完成的关键能力

- `RedisQueueBackend`：任务数据 / ready queue / delayed queue 使用真实 Redis 结构
- `RedisLockBackend`：支持真实 lease 获取、释放、续期与锁存活判断
- `RedisCompletionDedupBackend`：完成态幂等 claim-once
- `WorkerRegistry`：worker heartbeat / TTL 存活判断
- `TaskConsumer`：lease heartbeat
- `TaskReconciler`：orphan recovery / distributed repair gating
- `TaskCompletionNode`：终态持久化优先、回调失败不回滚终态
- executor / consumer / worker：retry exhaustion 语义已经对齐收敛
- observability：已有 `/debug/summary`（含 `anomaly_summary`）、`/debug/leases`、`/debug/leases/anomalies`、`/workers/<worker_id>/leases` 等排障端点

### 当前验证覆盖

- 单元测试与普通集成测试
- shared-client / fallback Redis integration tests
- live Redis smoke / recovery / overlap / consumer-recovery / retry-exhaustion 验证
- control-point transient failure hardening (lock/queue/registry/completion/worker)
- DAG 分支语义（fan-out/fan-in / partial success / cancellation-failure interplay）
- delayed promotion under concurrent load
- multi-control-point partition-like simulation
- multi-worker overlap / dead-owner recovery / delayed promotion 验证
- finalize / callback / reconciler overlap fault-injection tests
- control-point failure hardening for heartbeat / completion dedupe / reconciler liveness lookup / requeue enqueue paths

这意味着当前仓库已经不再是“只有 Redis-shaped 接口”的过渡原型，而是已经具备真正多进程 / 多节点部署所需的关键语义与一组比较扎实的验证护栏。

## Backend 抽象层（Batch 1 - 已完成）

框架现在引入了 **Backend 抽象层**，为分布式 deepwiki 架构的对齐做准备。当前实现支持：

### 抽象接口

- **QueueBackend** - 任务队列后端抽象
  - `enqueue()` / `dequeue()` / `peek()` / `cancel()` / `update_priority()`
  - 支持优先级队列和延时任务
  - 当前实现：`InMemoryQueueBackend`（使用 `asyncio.PriorityQueue`）与 `RedisQueueBackend`（真实 Redis 数据结构）

- **LockBackend** - 分布式锁后端抽象
  - `acquire()` / `release()` / `extend()` / `is_locked()`
  - 当前实现：`InMemoryLockBackend`（使用 `asyncio.Lock`）与 `RedisLockBackend`（真实 lease / TTL / compare-and-act）

- **RegistryBackend** - 调度注册表后端抽象
  - `create()` / `get()` / `list_active()` / `list_ready()`
  - `advance_next_fire()` / `pause()` / `resume()`
  - 当前实现：`InMemoryRegistryBackend`（使用 SQLite 持久化）
  - 注：schedule registry 仍以本地持久化为主，分布式关键共享状态当前主要集中在 queue / lock / completion dedupe / worker registry

### 使用方式

```python
from async_scheduler.backends import BackendConfig, BackendFactory

# 使用默认内存后端（当前行为）
from async_scheduler.platform import build_service_container
services = await build_service_container()

# 配置自定义后端（未来支持 Redis 等）
config = BackendConfig(
    queue_type="redis",
    lock_type="redis",
    registry_type="memory",
    redis_url="redis://localhost:6379/0",
    lease_ttl_seconds=30,
    heartbeat_interval_seconds=10,
)
services = await build_service_container(backend_config=config)

# 当前阶段说明：
# - 这会启用 distributed_settings 配置通路
# - 当前 queue / lock / completion dedupe / worker registry 已支持真实 async Redis client 路径
# - queue promotion 与 lock compare-and-act 等关键路径已具备 Lua/CAS 风格原子语义
# - 若环境未提供 redis client / live backend，仍可退回测试友好的 fallback 语义
# - 更完整的生产级运行时硬化、压测与外部 side-effect 交付保证仍在后续阶段
```

## StepExecutors 增强（Batch 2 - 已完成）

Batch 2 加强了 StepExecutors 组件，使其成为 DAG 执行的核心：

### 新增功能

- **ExecutionStatus** - 步骤执行状态枚举（pending, running, completed, failed, timeout, cancelled）
- **StepExecutionResult** - 包含状态、值、错误、执行时间等详细信息的执行结果
- **ExecutionMetrics** - 跟踪成功率和平均执行时间
- **异步执行支持** - `get_async_result()` 和 `cancel_execution()` 用于管理异步执行
- **指标收集** - 可选的执行指标跟踪

### 使用示例

```python
from async_scheduler.dag import StepExecutors, ExecutionMode, StepExecutionContext

executors = StepExecutors(enable_metrics=True)

# 执行步骤
ctx = StepExecutionContext(
    task_type="my_task",
    payload={"data": "value"},
    timeout_seconds=30,
)

result = await executors.execute(ExecutionMode.SYNC, ctx, handler)
print(f"Status: {result.status}, Value: {result.value}, Duration: {result.duration_ms}ms")

# 获取指标
metrics = executors.get_metrics()
print(f"Success rate: {metrics.get_success_rate()}%")
```

## ScheduleRegistry 生命周期扩展（Batch 2 - 已完成）

Batch 2 扩展了 ScheduleRegistry 的生命周期控制能力：

### 新增操作

- `delete(schedule_id)` - 删除调度
- `update(schedule_id, **updates)` - 更新调度属性
- `pause_all(tenant_id=None)` - 批量暂停（可选租户范围）
- `resume_all(tenant_id=None)` - 批量恢复（可选租户范围）
- `delete_all(tenant_id=None, status=None)` - 批量删除
- `get_count(status=None)` - 按状态计数
- `exists(schedule_id)` - 检查调度是否存在
- `get_by_name(name, tenant_id=None)` - 按名称查找

## 平台组件集成（Batch 3 - 已完成）

Batch 3 加强了 TaskCompletionNode 和 TaskReconciler 作为平台组件的集成：

### TaskCompletionNode

- **CompletionMetrics** - 跟踪完成统计和回调成功率
- **Completion Handlers** - 注册自定义完成处理逻辑
- **改进的错误处理** - 更好的日志记录和错误跟踪

### TaskReconciler

- **ReconciliationConfig** - 可配置的修复行为
- **ReconciliationMetrics** - 详细的修复统计
- **RepairStrategy** - 支持多种修复策略（mark_failed, requeue, ignore）
- **Reconciliation Handlers** - 注册自定义修复处理逻辑
- **孤立任务检测** - 检测长时间处于 queued 状态的任务

### CapabilityRegistry

- **增强的元数据** - 版本、作者、schema、时间戳
- **启用/禁用** - 在不注销的情况下禁用能力
- **使用跟踪** - 执行次数和最后执行时间
- **标签搜索** - 按标签发现能力
- **指标** - 注册表级别统计信息

### 使用示例

```python
from async_scheduler.platform import (
    TaskCompletionNode, TaskReconciler,
    ReconciliationConfig, RepairStrategy,
    CapabilityRegistry
)

# 创建增强的组件
completion_node = TaskCompletionNode(enable_metrics=True)

# 配置 reconciler
reconciler = TaskReconciler(
    config=ReconciliationConfig(
        stuck_after_seconds=3600,
        repair_strategy=RepairStrategy.MARK_FAILED,
    ),
    completion_node=completion_node,
)

# 注册处理程序
def on_task_completed(task):
    print(f"Task {task.id} completed with status {task.status}")

completion_node.register_completion_handler(TaskStatus.SUCCESS, on_task_completed)

# 增强的 capability registry
registry = CapabilityRegistry()
registry.register(
    "my_capability",
    handler,
    description="My custom capability",
    version="1.0.0",
    tags=["custom", "v1"],
    enabled=True,
)
```

## 运行与架构参考文档

- `docs/runtime/distributed-deployment-guide.md`：如何以 true distributed mode 运行当前仓库（含单机多进程 / 小规模多节点示例）
- `docs/reference/non-critical-shared-state-boundary.md`：哪些路径必须共享状态，哪些路径可以继续保持本地/聚合视图
- `docs/reference/deepwiki-distributed-architecture-reference.md`：deepwiki 对齐状态、剩余 gap 与下一步建议

## DeepWiki 分布式对齐路线图

| 阶段 | 状态 | 内容 |
|------|------|------|
| **Batch 1** | ✅ 已完成 | Backend 抽象层 + 内存实现，保持现有 API/CLI 行为不变 |
| **Batch 2** | ✅ 已完成 | StepExecutors 增强 + ScheduleRegistry 生命周期扩展 |
| **Batch 3** | ✅ 已完成 | TaskCompletionNode / TaskReconciler 集成 + CapabilityRegistry 增强 |
| **Batch 4** | ✅ 已完成 | 可用性硬化：文档、API、验证套件、observability 补齐 |
| **当前增量** | ✅ 已完成 | real Redis 关键路径、retry 语义收敛、lease observability、fault injection recovery tests |
| **未来** | 🔜 待规划 | 更完整压测矩阵、生产级 side-effect 交付链路、更多运行时硬化 |