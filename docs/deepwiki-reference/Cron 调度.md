# Cron 调度

## 概念

`CronScheduler` 在后台循环检查已注册的 `Schedule`，到达触发时间时自动创建任务并入队。每个 Schedule 有去重窗口（`dedup_window_seconds`），防止重复触发。

## 创建调度

```bash
curl -X POST http://127.0.0.1:8000/schedules \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "daily-report",
    "cron_expression": "0 9 * * *",
    "dedup_window_seconds": 300,
    "task_template": {
      "capability": "report_generator",
      "type": "daily"
    }
  }'
```

## Cron 表达式格式

标准 5 字段格式（基于 `croniter`）：

```
* * * * *
│ │ │ │ └── 星期（0-7，0 和 7 均为周日）
│ │ │ └──── 月（1-12）
│ │ └────── 日（1-31）
│ └──────── 时（0-23）
└────────── 分（0-59）
```

常用示例：

| 表达式 | 含义 |
|--------|------|
| `*/5 * * * *` | 每 5 分钟 |
| `0 9 * * 1-5` | 工作日早 9:00 |
| `0 0 1 * *` | 每月 1 日午夜 |
| `0 */2 * * *` | 每 2 小时整点 |

## CLI 创建调度

```bash
async-scheduler schedule heartbeat '*/5 * * * *' \
  --payload '{"capability":"echo","message":"tick"}'
```

## 调度管理

```bash
# 查看所有调度
curl http://127.0.0.1:8000/schedules

# 暂停调度
curl -X POST http://127.0.0.1:8000/schedules/{id}/pause

# 恢复调度
curl -X POST http://127.0.0.1:8000/schedules/{id}/resume

# 立即触发一次
curl -X POST http://127.0.0.1:8000/schedules/{id}/trigger
```

## ScheduleRegistry 生命周期 API

```python
from async_scheduler.scheduler import ScheduleRegistry

registry = ScheduleRegistry()

# 暂停 / 恢复
await registry.pause(schedule_id)
await registry.resume(schedule_id)

# 删除
await registry.delete(schedule_id)

# 按名称查询
schedule = await registry.get_by_name("daily-report", tenant_id="tenant-a")

# 批量操作
await registry.pause_all(tenant_id="tenant-a")
await registry.resume_all()
await registry.delete_all(status="paused")

# 统计
count = await registry.get_count(status="active")
exists = await registry.exists(schedule_id)
```

## 去重机制

`dedup_window_seconds` 决定同一 Schedule 在多短时间内不会重复触发。例如设为 `300`（5 分钟），即使因服务重启导致触发时间被重新计算，也不会在 5 分钟内生成第二个任务。
