# Docker 部署

本文档给出 Ray Async Framework 的 **分服务 Docker 部署方案**。

## 服务拆分

- `api`：对外 HTTP API（FastAPI）
- `worker`：任务消费执行
- `scheduler`：Cron 调度
- `reconciler`：后台修复/补偿
- `redis`：共享队列 / 分布式锁 / 协调状态
- `mysql`：任务元数据持久化

> 当前 compose 默认已切换为 **MySQL 8**，更适合分服务部署与后续扩容。

## 一键启动

```bash
docker compose up -d --build
```

查看状态：

```bash
docker compose ps
docker compose logs -f api
docker compose logs -f worker
docker compose logs -f scheduler
docker compose logs -f reconciler
```

访问：

- API: `http://127.0.0.1:8000`
- Swagger: `http://127.0.0.1:8000/docs`
- Health: `http://127.0.0.1:8000/health`

## 服务说明

### api

```bash
ray-async api --host 0.0.0.0 --port 8000 --init-db
```

### worker

```bash
ray-async worker --workers 2 --max-concurrent 16 --init-db
```

如需扩容：

```bash
docker compose up -d --scale worker=3
```

### scheduler

```bash
ray-async scheduler-service --poll-interval 15 --init-db
```

### reconciler

```bash
ray-async reconciler-service --interval 20 --init-db
```

## 关键环境变量

- `DATABASE_URL`：数据库连接串（默认 MySQL）
- `REDIS_URL`：Redis 地址
- `QUEUE_TYPE=redis`
- `LOCK_TYPE=redis`
- `REGISTRY_TYPE=memory`
- `LOG_FORMAT=json`
- `LOG_LEVEL=INFO`
- `SERVICE_NAME=<service-name>`

## 生产建议

### 1. 数据库

当前 compose 默认：

```bash
mysql+asyncmy://ray_async:ray_async@mysql:3306/ray_async
```

若使用外部 MySQL：

```bash
DATABASE_URL=mysql+asyncmy://user:pass@mysql-host:3306/ray_async
```

### 2. Redis

生产建议：
- 开启持久化
- 配认证
- 独立部署，不与应用容器混跑

### 3. worker 扩缩容

优先横向扩容 `worker`：

```bash
docker compose up -d --scale worker=5
```

### 4. 镜像构建

```bash
docker build -t ray-async-framework:latest .
```

如果要分别标记：

```bash
docker tag ray-async-framework:latest ray-async-framework:api
docker tag ray-async-framework:latest ray-async-framework:worker
docker tag ray-async-framework:latest ray-async-framework:scheduler
docker tag ray-async-framework:latest ray-async-framework:reconciler
```

本质上仍然是同一份基础镜像，不同服务通过不同 `command` 启动。

## 下一步

如果你准备走真正生产部署，下一步建议做：
1. PostgreSQL 替换 SQLite
2. 健康检查与 readiness/liveness
3. 镜像多阶段构建瘦身
4. Helm / K8s manifests
5. 日志直接输出到 ES / Loki / Fluent Bit 链路
