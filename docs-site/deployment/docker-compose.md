# Docker 部署

本文档描述当前仓库的 Docker / Compose 部署方式，已与代码现状对齐。

## 当前服务拆分

### 1. ops-api
- 入口：`src.main:app`
- 端口：`8000`
- 职责：运维管理面
- 路由：`/ops/v1/*` + `/health/*`
- 后台任务：`CronScheduler`
- 依赖：Redis（不要求 MySQL）

### 2. task-api
- 入口：`src.main_tasks:app`
- 端口：`8001`
- 职责：任务提交、查询、取消、结果获取
- 路由：`/tasks/*` + `/health/*`
- 后台任务：`TaskReconciler`
- 依赖：Redis + MySQL

### 3. redis
- 队列、注册表、锁、协调状态

### 4. mysql
- `TaskRecord` 持久化

### 5. observability（可选 profile）
- `jaeger`
- `prometheus`
- `grafana`

## 启动示例

### 启动核心服务

```bash
docker compose up -d ops-api task-api redis mysql
```

### 启动完整环境（含观测）

```bash
docker compose --profile observability up -d
```

### 查看日志

```bash
docker compose logs -f ops-api
docker compose logs -f task-api
docker compose logs -f redis
docker compose logs -f mysql
```

## 访问地址

- Ops API: `http://127.0.0.1:8000`
- Task API: `http://127.0.0.1:8001`
- Ops Swagger: `http://127.0.0.1:8000/docs`
- Task Swagger: `http://127.0.0.1:8001/docs`
- Ops Health: `http://127.0.0.1:8000/health/ready`
- Task Health: `http://127.0.0.1:8001/health/ready`

## 当前 compose 里的关键配置

### ops-api

- `BACKGROUND__RECONCILE__ENABLED=false`
- `BACKGROUND__CRON__ENABLED=true`

### task-api

- `BACKGROUND__RECONCILE__ENABLED=true`
- `BACKGROUND__CRON__ENABLED=false`

这是推荐生产分工：
- cron 调度只在 ops-api 跑
- reconcile 修复只在 task-api 跑

## 常用命令

### 仅启动 ops-api

```bash
docker compose up -d ops-api redis
```

### 仅启动 task-api

```bash
docker compose up -d task-api redis mysql
```

### 运行测试容器

```bash
docker compose --profile test run --rm test
```

## 环境变量

主要环境变量由 `docker-compose.yml` 中的 `x-common-env` 提供：

- `REDIS_URL`
- `MYSQL_URL`
- `ENVIRONMENT`
- `OTEL_EXPORTER_OTLP_ENDPOINT`
- `OTEL_SERVICE_NAME`

可通过 `.env` 覆盖。

## 生产建议

### 1. Redis
- 开启持久化
- 配置认证
- 与应用容器分离部署

### 2. MySQL
- task-api 才真正依赖 MySQL
- 若 MySQL 不可用，task-api 的任务持久化与查询能力会受影响

### 3. 扩缩容建议
- `task-api` 通常按请求量水平扩容
- `ops-api` 通常保持少量实例即可
- `CronScheduler` 和 `TaskReconciler` 都依赖 Redis 协调，避免多个实例无约束重复执行

### 4. 镜像
当前默认使用同一基础镜像，通过不同 `command` 启动不同服务。

例如：
```bash
python -m uvicorn src.main:app --host 0.0.0.0 --port 8000
python -m uvicorn src.main_tasks:app --host 0.0.0.0 --port 8001
```

## 说明

旧文档中提到的这些角色已不再对应当前 compose：
- `api`
- `worker`
- `scheduler`
- `reconciler`

当前仓库的实际对外 HTTP 服务是：
- `ops-api`
- `task-api`
