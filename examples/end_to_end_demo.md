# End-to-End Demo

下面是一套最小端到端演示流程。

## 1. 初始化数据库

```bash
cd /home/gem/.openclaw/workspace/projects/ray-async-framework
ray-async init-db-cmd --force
```

## 2. 启动 API

```bash
ray-async api --init-db
```

## 3. 创建租户

```bash
curl -X POST http://127.0.0.1:8000/tenants \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "team-a",
    "config": {"max_queued": 20, "max_running": 5}
  }'
```

## 4. 创建普通任务

```bash
curl -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "compute-task",
    "tenant_id": "tenant-a",
    "idempotency_key": "demo-task-1",
    "payload": {
      "capability": "compute",
      "operation": "add",
      "a": 1,
      "b": 2
    }
  }'
```

## 5. 创建 Schedule

```bash
curl -X POST http://127.0.0.1:8000/schedules \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "heartbeat",
    "cron_expression": "*/5 * * * *",
    "dedup_window_seconds": 60,
    "task_template": {"capability": "echo", "message": "tick"}
  }'
```

## 6. 查看能力列表

```bash
curl http://127.0.0.1:8000/capabilities
```

## 7. 跑一次 Reconciler

```bash
curl -X POST http://127.0.0.1:8000/reconciler/run
```

## 8. Redis 过渡态测试入口

如果你在推进当前仓库的 Redis 过渡态分布式能力，可以直接运行：

```bash
pytest -q tests/integration/test_real_redis_coordination.py
pytest -q tests/integration/test_real_redis_coordination_more.py
pytest -q tests/integration/test_real_redis_recovery_invariants.py
```

如果环境中可用 `fakeredis.aioredis`，还可以额外运行：

```bash
pytest -q tests/integration/test_fakeredis_coordination.py
```

说明：
- 上述测试主要验证 queue / lock / worker registry / completion dedupe 的 Redis 过渡态协调路径
- `test_fakeredis_coordination.py` 在缺少对应 fakeredis 模块时会自动 skip
- 当前这些测试是 distributed transition validation，不等于生产级 live Redis 验证
