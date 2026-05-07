<!-- 中文文档 -->
# 监控与告警配置

## Prometheus 指标

### 暴露的指标端点
- `GET /api/v1/health/metrics` - Prometheus 格式指标
- `GET /api/v1/health/ready` - 就绪检查（含 Redis/MySQL 连接性）
- `GET /api/v1/health/live` - 存活检查

### 关键业务指标

#### 队列指标
```
# 每个 capability 的待处理任务数
scheduler_queue_pending_tasks{capability="image_generate"} 42

# 每个 capability 的运行中任务数  
scheduler_queue_running_tasks{capability="image_generate"} 8

# 队列深度告警阈值
- warning: > 100
- critical: > 500
```

#### 任务执行指标
```
# 任务创建总数
scheduler_tasks_created_total{capability="image_generate",priority="normal"} 1500

# 任务完成总数（按状态）
scheduler_tasks_completed_total{capability="image_generate",status="success"} 1450
scheduler_tasks_completed_total{capability="image_generate",status="failure"} 50

# 任务执行时长分布（秒）
scheduler_task_execution_duration_seconds_bucket{capability="image_generate",status="success",le="0.1"} 100
scheduler_task_execution_duration_seconds_bucket{capability="image_generate",status="success",le="1.0"} 850
scheduler_task_execution_duration_seconds_sum{capability="image_generate",status="success"} 1250.5
scheduler_task_execution_duration_seconds_count{capability="image_generate",status="success"} 1450
```

#### DAG 执行指标
```
# DAG 执行时长
scheduler_dag_execution_duration_seconds_bucket{dag_id="pipeline_1",le="5.0"} 10
```

#### 系统指标
```
# 服务运行时间
scheduler_uptime_seconds 86400

# HTTP 请求总数
scheduler_request_count 15000

# 内存使用
scheduler_memory_rss_bytes 268435456

# CPU 时间
scheduler_cpu_seconds_total 125.5

# Redis 连接数
scheduler_redis_connections 5

# MySQL 连接数  
scheduler_mysql_connections 10
```

## Prometheus 配置示例

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'async-scheduler'
    scrape_interval: 15s
    static_configs:
      - targets:
        - 'task-api:8001'   # Task API 指标
        - 'ops-api:8000'    # Ops API 指标
    metrics_path: /api/v1/health/metrics
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
      - source_labels: [__address__]
        regex: '([^:]+):\d+'
        target_label: service
```

## Grafana 仪表板配置

### 建议的面板

1. **队列健康度**
   - 每个 capability 的待处理任务数（柱状图）
   - 每个 capability 的运行中任务数（线图）
   - 队列深度趋势（24小时）

2. **任务吞吐量**
   - 任务创建速率（requests/sec）
   - 任务完成速率（按状态）
   - 成功率（成功数/总数）

3. **执行时长**
   - P50/P95/P99 执行时长（按 capability）
   - 慢任务告警（> 30秒）

4. **系统资源**
   - 内存使用
   - CPU 使用率
   - Redis/MySQL 连接数

### Grafana 查询示例

```sql
-- 队列深度告警
max(scheduler_queue_pending_tasks) by (capability) > 100

-- 成功率下降告警
rate(scheduler_tasks_completed_total{status="failure"}[5m]) / 
rate(scheduler_tasks_completed_total[5m]) > 0.05  # 失败率 > 5%

-- 慢任务告警
histogram_quantile(0.95, rate(scheduler_task_execution_duration_seconds_bucket[5m])) > 30
```

## 告警规则 (Prometheus Alertmanager)

```yaml
groups:
  - name: scheduler_alerts
    rules:
      # 队列积压
      - alert: QueueBacklogHigh
        expr: max(scheduler_queue_pending_tasks) by (capability) > 100
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High queue backlog for {{ $labels.capability }}"
          description: "Queue {{ $labels.capability }} has {{ $value }} pending tasks"
      
      # 高失败率
      - alert: HighTaskFailureRate
        expr: |
          rate(scheduler_tasks_completed_total{status="failure"}[5m])
          / ignoring(status)
          rate(scheduler_tasks_completed_total[5m]) > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High failure rate for tasks"
          description: "Task failure rate is {{ $value | humanizePercentage }}"
      
      # 服务不可用
      - alert: ServiceDown
        expr: up{job="async-scheduler"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.instance }} is down"
          description: "Service has been down for more than 1 minute"
      
      # Redis 连接失败
      - alert: RedisUnavailable
        expr: scheduler_redis_connections == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Redis connection lost"
          description: "Cannot connect to Redis for 2 minutes"
      
      # 慢任务
      - alert: SlowTasks
        expr: |
          histogram_quantile(0.95, rate(scheduler_task_execution_duration_seconds_bucket[5m])) > 30
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Slow tasks detected"
          description: "95th percentile task execution > 30s"
```

## 日志聚合

### 结构化日志字段
```json
{
  "timestamp": "2026-05-05T10:00:00Z",
  "level": "INFO",
  "logger": "src.platform.queue_manager",
  "message": "Task enqueued",
  "task_id": "task_123",
  "capability": "image_generate",
  "queue_position": 5,
  "pending_count": 42
}
```

### 建议的日志查询
- `level:ERROR` - 所有错误
- `capability:"image_generate" AND queue_position:>100` - 高队列位置
- `status:"failure"` - 失败的任务

## 健康检查配置

### Kubernetes 探针
```yaml
# task-api deployment
containers:
  - name: task-api
    livenessProbe:
      httpGet:
        path: /api/v1/health/live
        port: 8001
      initialDelaySeconds: 30
      periodSeconds: 10
    readinessProbe:
      httpGet:
        path: /api/v1/health/ready
        port: 8001
      initialDelaySeconds: 5
      periodSeconds: 5

# ops-api deployment  
containers:
  - name: ops-api
    livenessProbe:
      httpGet:
        path: /ops/v1/health
        port: 8000
      initialDelaySeconds: 30
      periodSeconds: 10
    readinessProbe:
      httpGet:
        path: /ops/v1/health
        port: 8000
      initialDelaySeconds: 5
      periodSeconds: 5
```

## 调试端点

### 任务调试
```bash
# 获取任务的完整状态
curl http://ops-api:8000/ops/v1/tasks/task_123/debug

# 响应示例
{
  "task_id": "task_123",
  "sources": {
    "mysql": {
      "status": "running",
      "created_at": "2026-05-05T10:00:00",
      "task_type": "image_generate"
    },
    "redis": {
      "found_in": [{
        "queue": "running",
        "capability": "image_generate"
      }]
    }
  },
  "recommendations": [
    "Task is currently running in image_generate."
  ]
}
```

## 部署检查清单

1. ✅ Prometheus 抓取配置
2. ✅ Grafana 数据源和仪表板
3. ✅ Alertmanager 告警规则
4. ✅ 日志聚合（ELK/Loki）
5. ✅ Kubernetes 探针配置
6. ✅ 监控告警测试
7. ✅ 容量规划（基于指标历史）