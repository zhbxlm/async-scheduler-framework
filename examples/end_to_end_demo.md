# End-to-End Demo

下面是一套最小端到端演示流程。

## 1. 初始化数据库

```bash
cd /home/gem/.openclaw/workspace/projects/async-scheduler-framework
async-scheduler init-db-cmd --force
```

## 2. 启动 API

```bash
async-scheduler api --init-db
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
