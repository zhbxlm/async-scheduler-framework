# DeepWiki 设计对齐状态

## 目的

本文档用于明确 Ray Async Framework 当前与 deepwiki 风格分布式调度平台之间的**对齐边界**，避免误判为“已等价实现”。

---

## 1. 已基本对齐（Core semantics aligned）

这些能力已经具备真实实现，不再只是接口占位：

### 1.1 分布式共享状态核心链路
- Redis-backed queue
- Redis-backed lease / lock
- worker registry + heartbeat TTL
- completion idempotency
- execution attempts
- distributed reconciler

### 1.2 分布式任务收敛语义
- lease loss 检测
- stale lease recovery
- dual reconciler single-repair behavior
- duplicate completion convergence
- callback overlap / retry semantics（轻量实现）

### 1.3 Operator / observability surface
- `/debug/leases`
- `/debug/leases/anomalies`
- `/debug/leases/anomalies/summary`
- `/workers/{worker_id}/leases`
- `/reconciler/history`
- `/debug/summary`
- `/async-proxy/stats`（含 `event_summary`）
- `/callbacks/stats`（含 `control_plane_summary`）
- `/resources/stats`（含 `workload_summary`）
- `/clusters/stats`（含 `workload_summary`）

---

## 2. 部分对齐（Partially aligned / scaffolded）

这些模块已经存在结构、接口或部分实现，但还不是 deepwiki 平台级等价能力：

### 2.1 Tenant quota / multi-tenant governance
当前已有：
- `TenantQuotaManager`
- queue/running/gpu/actor quota 抽象
- 部分 Redis 路径
- `/quota/stats` 的 tenant + summary 观测面

仍欠缺：
- 更严格的 Redis 原子 check-and-increment 主路径
- 更完整的 operator observability
- 更强的一致性/回收语义

### 2.2 ActorPool / ResourceManager / ClusterRegistry
当前已有：
- 模块骨架
- 部分可运行逻辑
- 与 service container 的接线
- ResourceManager summary / scale history 观测面
- ClusterRegistry stats / recent routing decisions 观测面

仍欠缺：
- 生产级资源决策闭环
- 更强的集群控制面
- 更完整的调度平台行为验证

### 1.4 WorkerRegistry 双结构架构（已修复 2026-05-04）
- hash 持久存储（无 TTL）用于 `list_workers(include_stale=True)` 枚举
- liveness key（pexpire TTL）用于 `is_live()` 判断
- deregister 同时清理两种结构
- Reconciler 在 `is_live()` 异常时跳过修复（安全优先）
- Completion vs reconciler attempt status race 通过 `only_if_status=RUNNING` 解决

---

### 2.3 Async Proxy / callback sidecar
当前已有：
- callback dispatcher
- async proxy sidecar 骨架
- 部分 retry / dead-letter 行为
- sidecar stats / recent events / publish-wait-timeout 观测面
- TaskConsumer → AsyncProxySidecar 的 running 事件自动发布链路
- TaskCompletionNode → AsyncProxySidecar 的终态事件自动发布链路
- sidecar 终态/运行中事件已携带 task_name / tenant_id / callback_url / completed_at / completion_kind / callback_delivery 等上下文
- CallbackDispatcher 已可提供 retry queue / dead-letter 的基础统计面与最近摘要，并暴露 `/callbacks/stats`
- `/async-proxy/stats` 已聚合 callback control-plane 摘要，且补充 `event_summary` 作为事件层稳定摘要入口
- `ResourceManager.get_stats()` 已补充 `workload_summary`，提供按 capability 的 pending/running/scheduled 视图
- `ClusterRegistry.get_stats_async()` / `/clusters/stats` 已补充 `workload_summary`，提供按 capability 的 pending/capacity/region 聚合视图

仍欠缺：
- 真 sidecar 级异步通知实现
- 更完整的 delivery guarantee 链路
- 更贴近 long-running/offload 的任务状态外包语义

---

## 3. 明确未对齐（Not implemented yet / not equivalent）

### 3.1 官方 deepwiki 平台等价能力
当前仓库**不是**以下系统的等价实现：
- deepwiki 生产级多节点调度平台
- 完整 RayData / NodeAgent / SchedulerActor 体系
- 完整外部副作用交付保障链

### 3.2 平台治理与控制面完整性
仍缺：
- 更完整的资源治理控制面
- 更系统化的 cluster operations
- 更强的 side-effect / callback delivery guarantees

---

## 4. 当前对齐状态（2026-05-04 更新）

### 已修复（原 xfail → 正常测试）
- WorkerRegistry TTL tracking（双结构修复）
- Completion vs reconciler attempt status race（compare-and-set）
- InMemoryQueueBackend delayed promotion 并发可靠性
- Reconciler `is_live` 异常时安全跳过修复
- Redis DELAYED_PROMOTE_CAP_SCRIPT Lua 脚本字段解析 bug（`"key": "value"` 格式）
- 测试 DB 隔离（conftest truncate before each test）

### 仍欠缺（不影响核心语义）
- CallbackDispatcher 真实 HTTP delivery（当前是伪实现占位）
- 更强的 TenantQuota cross-worker 原子性测试
- ActorPool / ResourceManager / ClusterRegistry 生产级控制面

---

## 5. 当前阶段结论

当前仓库已经具备：
- 一个可验证的分布式调度内核
- 真实 Redis 关键共享状态路径
- 本机 MariaDB + Redis 下的核心测试验证

但它仍然是：

> **向 deepwiki 风格平台持续收敛的工程化骨架**

而不是 deepwiki 平台的生产级等价实现。
