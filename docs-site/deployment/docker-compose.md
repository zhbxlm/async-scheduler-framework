# Docker Compose Deployment

Deploy Async Scheduler with a complete monitoring stack using Docker Compose.

## Quick Start

```bash
# Clone the repository
git clone https://github.com/zhbxlm/async-scheduler-framework.git
cd async-scheduler-framework

# Start all services
docker-compose -f docker-compose.monitoring.yml up -d

# Check services
docker-compose -f docker-compose.monitoring.yml ps
```

## Service Architecture

The Docker Compose stack includes:

| Service | Port | Purpose |
|---------|------|---------|
| **redis** | 6379 | In-memory data store for queues |
| **mysql** | 3306 | Persistent task storage |
| **task-api** | 8001 | Task API (business logic) |
| **ops-api** | 8000 | Ops API (monitoring & management) |
| **prometheus** | 9090 | Metrics collection & alerting |
| **grafana** | 3000 | Metrics visualization dashboards |
| **alertmanager** | 9093 | Alert routing & notification |
| **node-agent** | - | Example worker node |
| **load-generator** | - | Load testing utility |

## Environment Configuration

### Required Environment Variables

```bash
# Redis configuration
REDIS_URL=redis://redis:6379/0

# MySQL configuration  
MYSQL_URL=mysql://scheduler:scheduler123@mysql:3306/scheduler

# API configuration
SERVER_PORT=8001  # or 8000 for ops-api
ENVIRONMENT=development
LOG_LEVEL=INFO
```

### Optional Environment Variables

```bash
# Multi-tenancy
TENANT_MULTI_TENANT_ENABLED=true
TENANT_SUPER_ADMIN_API_KEY=your-secret-key
TENANT_TENANT_ID_HEADER=X-Tenant-ID

# Task configuration
TASK_DEFAULT_PRIORITY=normal
TASK_MAX_RETRIES=3

# Queue configuration
QUEUE_MAX_DEPTH=1000
QUEUE_MAX_CONCURRENT=8
QUEUE_TTL=3600

# Cron scheduler
CRON_INTERVAL_SECONDS=30
CRON_BATCH_SIZE=100
```

## Health Checks

All services include health checks:

```bash
# Check service health
curl http://localhost:8001/api/v1/health

# Check Redis health
curl http://localhost:8001/api/v1/health/redis

# Check MySQL health  
curl http://localhost:8001/api/v1/health/mysql

# Prometheus metrics
curl http://localhost:8001/api/v1/health/metrics
```

## Monitoring Stack

### Prometheus

Access Prometheus at [http://localhost:9090](http://localhost:9090):

- **Targets**: Check service discovery status
- **Graph**: Query metrics with PromQL
- **Alerts**: View active alert rules
- **Status**: Service discovery and configuration

### Grafana

Access Grafana at [http://localhost:3000](http://localhost:3000):

- **Username**: `admin`
- **Password**: `admin`

Pre-configured dashboards:
1. **Queue Health** - Pending/running tasks per capability
2. **Task Throughput** - Creation/completion rates
3. **Execution Duration** - P50/P95/P99 latency
4. **System Resources** - CPU, memory, connections

### Alertmanager

Access Alertmanager at [http://localhost:9093](http://localhost:9093):

- **Alerts**: View and silence alerts
- **Status**: Check alert routing
- **Silences**: Manage alert suppression

## Data Persistence

Volumes are configured for data persistence:

```yaml
volumes:
  redis_data:    # Redis AOF persistence
  mysql_data:    # MySQL data directory  
  prometheus_data:  # Prometheus time-series data
  grafana_data:  # Grafana dashboards & config
  alertmanager_data:  # Alertmanager state
```

To backup data:

```bash
# Backup MySQL
docker exec async-scheduler-framework-mysql-1 mysqldump -u scheduler -pscheduler123 scheduler > backup.sql

# Backup Redis
docker exec async-scheduler-framework-redis-1 redis-cli save
docker cp async-scheduler-framework-redis-1:/data/dump.rdb ./redis-backup.rdb
```

## Scaling

### Horizontal Scaling

```yaml
# Scale task-api instances
task-api:
  deploy:
    replicas: 3
  environment:
    - SERVER_HOST=0.0.0.0
    - REDIS_URL=redis://redis:6379/0
    - MYSQL_URL=mysql://scheduler:scheduler123@mysql:3306/scheduler

# Scale ops-api instances
ops-api:
  deploy:
    replicas: 2
  environment:
    - SERVER_HOST=0.0.0.0  
    - REDIS_URL=redis://redis:6379/0
```

### Load Balancer Configuration

```yaml
nginx:
  image: nginx:alpine
  ports:
    - "80:80"
  volumes:
    - ./nginx.conf:/etc/nginx/nginx.conf
  depends_on:
    - task-api
    - ops-api
```

Example `nginx.conf`:

```nginx
upstream task_api {
  least_conn;
  server task-api:8001;
}

upstream ops_api {
  least_conn;
  server ops-api:8000;
}

server {
  listen 80;
  
  location /api/v1/ {
    proxy_pass http://task_api;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
  }
  
  location /ops/v1/ {
    proxy_pass http://ops_api;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
  }
}
```

## Production Considerations

### Security

```yaml
# Use secrets for sensitive data
secrets:
  mysql_root_password:
    file: ./secrets/mysql_root_password.txt
  redis_password:
    file: ./secrets/redis_password.txt

mysql:
  environment:
    MYSQL_ROOT_PASSWORD_FILE: /run/secrets/mysql_root_password
  secrets:
    - mysql_root_password

redis:
  command: redis-server --requirepass $$(cat /run/secrets/redis_password)
  secrets:
    - redis_password
```

### Network Isolation

```yaml
networks:
  backend:
    driver: bridge
    internal: true  # Isolate from external traffic
  
  monitoring:
    driver: bridge

# Only expose necessary ports
task-api:
  networks:
    - backend
    - monitoring
  ports:
    - "8001:8001"  # Expose only to load balancer

prometheus:
  networks:
    - monitoring
  ports:
    - "9090:9090"  # Internal monitoring only
```

### Resource Limits

```yaml
task-api:
  deploy:
    resources:
      limits:
        cpus: '1'
        memory: 512M
      reservations:
        cpus: '0.5'
        memory: 256M

redis:
  deploy:
    resources:
      limits:
        cpus: '0.5'
        memory: 256M
      reservations:
        cpus: '0.25'
        memory: 128M
```

## Troubleshooting

### Common Issues

1. **Services not starting**
   ```bash
   # Check logs
   docker-compose -f docker-compose.monitoring.yml logs task-api
   
   # Check health
   docker-compose -f docker-compose.monitoring.yml ps
   ```

2. **Database connection errors**
   ```bash
   # Wait for MySQL to be ready
   docker-compose -f docker-compose.monitoring.yml exec mysql mysqladmin ping -h localhost -u root -pscheduler123
   
   # Check MySQL logs
   docker-compose -f docker-compose.monitoring.yml logs mysql
   ```

3. **Redis connection errors**
   ```bash
   # Test Redis connection
   docker-compose -f docker-compose.monitoring.yml exec redis redis-cli ping
   
   # Check Redis logs
   docker-compose -f docker-compose.monitoring.yml logs redis
   ```

### Maintenance Commands

```bash
# View logs
docker-compose -f docker-compose.monitoring.yml logs -f

# Restart services
docker-compose -f docker-compose.monitoring.yml restart task-api

# Scale services
docker-compose -f docker-compose.monitoring.yml up -d --scale task-api=3

# Clean up
docker-compose -f docker-compose.monitoring.yml down -v

# Update images
docker-compose -f docker-compose.monitoring.yml pull
docker-compose -f docker-compose.monitoring.yml up -d
```

## Next Steps

- [Kubernetes Deployment](kubernetes.md) - Deploy to Kubernetes
- [Monitoring Configuration](../guides/monitoring.md) - Customize monitoring
- [Production Checklist](../guides/production-checklist.md) - Production readiness