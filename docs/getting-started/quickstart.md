# Quickstart: Run the async-scheduler-framework in 5 minutes

This guide walks through the fastest path to get the async‑scheduler‑framework running in a split deployment (`ops‑api` + `task‑api`) with a minimal local Redis+MySQL stack.

---

## 1. Prerequisites

- **Python 3.10+** (`python3 --version`)
- **pip** (or uv) to install dependencies
- **Docker** (optional, for Redis+MySQL containers)
- **Git**

## 2. Clone and install

```bash
git clone https://github.com/zhbxlm/async-scheduler-framework.git
cd async-scheduler-framework

# Install runtime + development dependencies
pip install -e .[dev]
```

## 3. Start Redis + MySQL (Docker)

If you don’t have Redis/MySQL already running, use the bundled `docker‑compose.yml`:

```bash
docker compose up -d redis mysql

# Wait a few seconds for services to be ready
docker compose ps
```

> Both services expose standard ports on `localhost`:  
> – Redis: `6379` (no password)  
> – MySQL: `3306` (user `root`, password `secret`, database `async_scheduler`)

## 4. Run split APIs (two terminal sessions)

### Terminal 1: **ops‑api** (port 8000, Redis only)
```bash
export REDIS_URL=redis://localhost:6379/0
export DEPLOYMENT_ROLE=ops-api
export SERVICE_NAME=scheduler-ops-api

python -m src.main
```

You should see:
```
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8000
```

### Terminal 2: **task‑api** (port 8001, Redis + MySQL)
```bash
export REDIS_URL=redis://localhost:6379/0
export MYSQL_URL=mysql://root:secret@localhost:3306/async_scheduler
export DEPLOYMENT_ROLE=task-api
export SERVICE_NAME=scheduler-task-api

python -m src.main_tasks
```

The `task‑api` will:
1. Auto‑create the `async_scheduler` database if not present
2. Create required tables (`tasks`, `tenants`, `capabilities`, etc.)
3. Start with background reconciler enabled

## 5. Test basic workflows

### Create a task (via task‑api)
```bash
curl -X POST http://localhost:8001/api/v1/tasks/ \\
  -H "Content-Type: application/json" \\
  -H "X-API-Key: placeholder" \\
  -d '{
    "task_type": "video_generation",
    "priority": "normal",
    "input_data": {"video_id": "vid_001"}
  }'
```

Response:
```json
{
  "task_id": "task-abc123...",
  "tenant_id": "",
  "status": "queued",
  "queue_position": 0,
  "idempotent_reused": false
}
```

### Check ops‑api overview
```bash
curl http://localhost:8000/ops/v1/overview
```

Returns Redis health + per‑capability queue stats.

### View health endpoints
- **ops‑api**: `http://localhost:8000/health`
- **task‑api**: `http://localhost:8001/health`

## 6. (Optional) Deploy with the bundled compose file

The repository includes a full `docker‑compose.yml` that starts both APIs + Redis + MySQL:

```bash
# Bring up the whole stack
docker compose up -d scheduler-ops-api scheduler-task-api redis mysql

# Check logs
docker compose logs -f scheduler-task-api
```

Access:
- ops‑api: `http://localhost:8000`
- task‑api: `http://localhost:8001`

## 7. Next steps

### Explore API docs
Both services expose OpenAPI (Swagger) docs:
- ops‑api: `http://localhost:8000/docs`
- task‑api: `http://localhost:8001/docs`

### Configure tenants + capabilities
See [`docs/configuration.md`](../configuration.md) for setting up:
- Tenant registry
- Capability definitions
- Priority weights

### Run existing tests
```bash
pytest -xvs
```

---

## Troubleshooting

### MySQL connection fails
- Ensure MySQL is reachable (`mysql -h localhost -u root -psecret async_scheduler`)
- The database `async_scheduler` will be created automatically if the user has `CREATE DATABASE` permission

### Redis not reachable
- Check `docker compose ps redis`
- Try `redis-cli ping`

### APIs fail to start (missing dependencies)
Run:
```bash
pip install -e .[dev]
```

### Want single‑API mode?
The framework can run in a single monolithic mode for development:
```bash
export REDIS_URL=redis://localhost:6379/0
export MYSQL_URL=mysql://root:secret@localhost:3306/async_scheduler
python -m src.main  # starts combined API on port 8000
```

However, **split deployment is recommended** for production‑like semantics.