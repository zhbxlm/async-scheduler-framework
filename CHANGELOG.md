# Changelog

All notable changes to async-scheduler-framework are documented here.

---

## [Unreleased] — 2026-05-08

### Changed

- **Phase 3 architecture normalization** completed:
  - standardized `clusters` / `nodes` / `dags` / `schedules` route layering,
  - added shared runtime assembly helpers for ops-api / task-api / control-plane,
  - reduced repeated `ServiceContainer` builder plumbing while preserving role ownership.
- Added focused regression coverage for:
  - resource routes,
  - runtime helper contracts,
  - container builder contracts.

### Internal

- New internal references:
  - `docs/internal/reference/2026-05-08-phase3-architecture-summary.md`
  - `docs/internal/reference/2026-05-08-phase3-release-notes.md`
  - `docs/internal/pr-description-phase3.md`

---

## [v1.1.0] — 2026-05-07

### New

- **8 独立可发布的用户/部署包**（`packages/` 目录）：
  - `async-scheduler-sdk` — Python HTTP 客户端 + pydantic 模型
  - `async-scheduler-worker` — `BaseWorker` 基类（**零依赖**，Ray 可选）
  - `async-scheduler-proxy` — 嵌入已有服务的 fire-and-forget Redis 代理
  - `async-scheduler-cli` — kubectl 风格 CLI（`scheduler` 命令）
  - `async-scheduler-task-api` — 任务提交 API 服务（port 8001）
  - `async-scheduler-ops-api` — 运维管理 API 服务（port 8000）
  - `async-scheduler-control-plane` — 独立 control-plane 后台工作进程
  - `async-scheduler-agent` — Worker 节点 Agent 服务
- **Task API 和 Ops API 独立部署**，权限和暴露范围分离
- 每个包各自有 `scheduler-*` CLI 入口、`README.md` 和独立 `pyproject.toml`
- `scripts/build_packages.sh` 支持一键构建全部 8 个包

### Changed

- 包命名空间从 `src.*` 彻底迁移到 `scheduler_*/`，用户不再感知框架内部模块
- `async-scheduler-cli` 完全自包含，不依赖 `src.*`
- `async-scheduler-proxy` 完全自包含，不依赖 `src.*`

### Fixed

- Redis HA 客户端重试/熔断/降级（P0）
- 告警系统 AlertRouter/Formatter/Notifier，16 条规则，Alertmanager 三级路由（P0）
- MySQL-first 原子写入协调器 + 补偿服务 + TaskReconciler（P0）
- 61 处未使用导入已清理（autoflake）
- F841 无效变量修复（dag_engine.py 等）
- AlertNotifier WEBHOOK / INFOFLOW / EMAIL 三通道实现
- alerts.py 5 个 TODO 替换为 Redis 实现（历史记录/静音/确认）

### CI

- 468/468 测试通过（Python 3.10 + 3.11）
- `build-packages` job 自动构建并上传 8 个包的 wheel artifacts

---

## [v1.0.0] — 2026-05-06

初始发布版本。

- 分布式异步任务调度框架，Ray 架构，Redis + MySQL 存储
- 支持 DAG 任务编排、Capability 路由、多租户、Cron 调度
- Prometheus 监控指标、OpenTelemetry 链路追踪
- FastAPI REST API（任务提交 + 运维管理）
- 194/194 单元测试通过
