<!-- 中文文档 -->
# 快速启动：5 分钟跑起来

本文带你以最快的方式启动 async-scheduler-framework 的拆分部署模式（`ops-api` + `task-api`），依赖本地 Redis + MySQL。

---

## 1. 环境要求

- **Python 3.10+**（`python3 --version`）
- **pip**（或 uv）用于安装依赖
- **Docker**（可选，用于快速启动 Redis + MySQL 容器）
- **Git**

## 2. 克隆并安装

```bash
git clone https://github.com/zhbxlm/async-scheduler-framework.git
cd async-scheduler-framework

# 安装运行时 + 开发依赖
pip install -e .[dev]
```

## 3. 启动 Redis + MySQL（Docker）

如果本机没有运行中的 Redis/MySQL，使用仓库自带的 `docker-compose.yml`：

```bash
docker compose up -d redis mysql

# 等待几秒，确认服务就绪
docker compose ps
```

> 两个服务均通过标准端口暴露在 `localhost`：
> - Redis：`6379`（无密码）
> - MySQL：`3306`（用户 `root`，密码 `secret`，数据库 `async_scheduler`）

## 4. 分别启动两个 API（两个终端）

### 终端 1：**ops-api**（端口 8000，仅依赖 Redis）

```bash
export REDIS_URL=redis://localhost:6379/0
export DEPLOYMENT_ROLE=ops-api
export SERVICE_NAME=scheduler-ops-api

python -m src.main
```

看到以下输出即表示启动成功：

```
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8000
```

### 终端 2：**task-api**（端口 8001，依赖 Redis + MySQL）

```bash
export REDIS_URL=redis://localhost:6379/0
export MYSQL_URL=mysql://root:secret@localhost:3306/async_scheduler
export DEPLOYMENT_ROLE=task-api
export SERVICE_NAME=scheduler-task-api

python -m src.main_tasks
```

task-api 启动时会自动：
1. 创建 `async_scheduler` 数据库（如果不存在）
2. 创建所需表（`tasks`、`tenants`、`capabilities` 等）
3. 启动后台 reconciler

## 5. 测试基本功能

### 提交任务（通过 task-api）

```bash
curl -X POST http://localhost:8001/api/v1/tasks/ \
  -H "Content-Type: application/json" \
  -H "X-API-Key: placeholder" \
  -d '{
    "task_type": "video_generation",
    "priority": "normal",
    "input_data": {"video_id": "vid_001"}
  }'
```

响应示例：

```json
{
  "task_id": "task-abc123...",
  "tenant_id": "",
  "status": "queued",
  "queue_position": 0,
  "idempotent_reused": false
}
```

### 查看系统概览（ops-api）

```bash
curl http://localhost:8000/ops/v1/overview
```

返回 Redis 健康状态与各 capability 的队列统计。

### 健康检查端点

- **ops-api**：`http://localhost:8000/health`
- **task-api**：`http://localhost:8001/health`

## 6. （可选）使用 Compose 一键启动

仓库包含完整的 `docker-compose.yml`，可同时启动两个 API + Redis + MySQL：

```bash
# 启动完整服务栈
docker compose up -d scheduler-ops-api scheduler-task-api redis mysql

# 查看日志
docker compose logs -f scheduler-task-api
```

访问地址：
- ops-api：`http://localhost:8000`
- task-api：`http://localhost:8001`

## 7. 下一步

### 查看 API 文档

两个服务均内置 OpenAPI（Swagger）文档：
- ops-api：`http://localhost:8000/docs`
- task-api：`http://localhost:8001/docs`

### 配置租户与能力

参见 [配置说明](../getting-started/configuration.md)，了解：
- 租户注册
- capability 定义
- 优先级权重配置

### 运行测试套件

```bash
pytest -xvs
```

---

## 常见问题排查

### MySQL 连接失败
- 确认 MySQL 可达：`mysql -h localhost -u root -psecret async_scheduler`
- 确保用户拥有 `CREATE DATABASE` 权限（数据库会自动创建）

### Redis 不可达
- 检查容器状态：`docker compose ps redis`
- 测试连通性：`redis-cli ping`

### 依赖缺失导致 API 启动失败

```bash
pip install -e .[dev]
```

### 想用单 API 模式？

框架支持单体模式，适合开发调试：

```bash
export REDIS_URL=redis://localhost:6379/0
export MYSQL_URL=mysql://root:secret@localhost:3306/async_scheduler
python -m src.main  # 在端口 8000 启动合并 API
```

> **生产环境建议使用拆分部署模式（ops-api + task-api）。**
