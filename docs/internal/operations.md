# Ray Async Operations Guide

## Architecture

```
clients -> task-api:8001 (task CRUD)
        -> ops-api:8000 (management)
              |
        Redis (queue + registry + lock)
              |
        MySQL (TaskRecord persistence)
              |
        Node Agents + Workers
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| REDIS_URL | Yes | redis://localhost:6379 | Redis connection URL |
| MYSQL_URL | Yes* | - | MySQL connection (task-api) |
| ENVIRONMENT | No | development | dev/staging/production |
| LOG_LEVEL | No | INFO | Logging level |
| LOG_JSON | No | true | JSON structured logging |
| BACKGROUND_RECONCILE_ENABLED | No | true | Enable task reconciler |
| BACKGROUND_CRON_ENABLED | No | false | Enable cron scheduler (task-api) |
| TENANT_SUPER_ADMIN_API_KEY | No | - | Super admin API key |
| TENANT_MULTI_TENANT_ENABLED | No | false | Enable multi-tenant mode |

*Required for task-api

## Deployment

### Docker Compose

```yaml
services:
  redis:
    image: redis:7-alpine
    ports: [6379:6379]
  mysql:
    image: mysql:8
    environment:
      MYSQL_ROOT_PASSWORD: testpass
      MYSQL_DATABASE: scheduler_test
    ports: [3306:3306]
  ops-api:
    build: .
    command: python -m uvicorn src.main:app --host 0.0.0.0 --port 8000
    ports: [8000:8000]
    environment:
      REDIS_URL: redis://redis:6379
      BACKGROUND_CRON_ENABLED: 'true'
  task-api:
    build: .
    command: python -m uvicorn src.main_tasks:app --host 0.0.0.0 --port 8001
    ports: [8001:8001]
    environment:
      REDIS_URL: redis://redis:6379
      MYSQL_URL: mysql://root:testpass@mysql/scheduler_test
      BACKGROUND_RECONCILE_ENABLED: 'true'
```

### Manual (systemd)

```ini
[Unit]
Description=Ray Async Ops API
After=network.target
[Service]
User=app
WorkingDirectory=/opt/async-scheduler
ExecStart=/opt/async-scheduler/.venv/bin/uvicorn src.main:app --host 0.0.0.0 --port 8000
Restart=always
[Install]
WantedBy=multi-user.target
```

## Monitoring

### Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| tasks_created_total | Counter | Tasks created |
| tasks_completed_total | Counter | Tasks completed |
| tasks_failed_total | Counter | Tasks failed |
| queue_depth | Gauge | Pending tasks |
| queue_running | Gauge | Running tasks |
| api_request_duration_seconds | Histogram | Request latency |
| circuit_breaker_state | Gauge | 0=closed, 1=half_open, 2=open |

### Health Check Commands

```bash
curl http://localhost:8000/health/ready
curl http://localhost:8001/health/ready
curl -H 'Authorization: Bearer KEY' http://localhost:8000/ops/v1/overview
```

## Troubleshooting

### Redis Down
Symptom: 503 on all endpoints
Fix: `redis-cli ping`, check connection

### MySQL Down
Symptom: task-api returns 503
Fix: `mysqladmin ping`, check disk/network

### Circuit Breaker Open
Symptom: tasks for capability rejected
Fix: Verify service health, reset manually via Redis:
```bash
redis-cli DEL queue:cb:CAPABILITY:cb:state
redis-cli DEL queue:cb:CAPABILITY:cb:failures
```

### Queue Full
Symptom: POST /tasks returns accepted=false
Fix: Increase max_queue_depth or add workers

### Stale Tasks
Symptom: tasks stuck in running status
Fix: Trigger reconciler: `POST /ops/v1/queue/CAP/cleanup-stale`

## Backup

```bash
# Redis
redis-cli BGSAVE

# MySQL
mysqldump -u root -p scheduler_test > backup.sql
```

## Scaling

- ops-api: stateless, scale horizontally
- task-api: stateless (state in Redis+MySQL), scale horizontally
- Redis: single instance for coordination
- MySQL: primary for writes, replicas for reads

Multi-instance deployment uses Redis Lua scripts for atomic global concurrency control.

## Logging

JSON structured format:
```json
{"timestamp":"...","level":"INFO","logger":"src.platform","message":"...","module":"..."}
```

Logs to stdout. Use `docker logs` or `journalctl` for systemd.