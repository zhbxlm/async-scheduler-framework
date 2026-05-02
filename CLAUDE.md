# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Installation
```bash
cd /home/gem/.openclaw/workspace/projects/async-scheduler-framework
python3 -m pip install -e .[dev] --no-build-isolation
```

### Database
```bash
# Initialize database
async-scheduler init-db-cmd --force
```

### Running the System
```bash
# Start API server
async-scheduler api --init-db

# Start dev mode (includes API + worker + scheduler)
async-scheduler dev --init-db

# Manual reconciler run
async-scheduler reconcile
```

### Testing
```bash
# Run all tests
pytest -q

# Run smoke test (framework validation)
python -m scripts.smoke_test

# Run single test
pytest tests/test_task_lifecycle.py -v

# Run integration tests
pytest tests/integration/ -v

# Run real-Redis specific tests
pytest tests/test_real_redis_*.py -v
```

### Linting & Type Checking
```bash
# Format and lint
ruff check async_scheduler/
ruff format async_scheduler/

# Type check
mypy async_scheduler/
```

## Architecture Overview

This is an async task scheduling framework with **distributed kernel semantics**. The framework supports both in-memory and Redis-backed backends through a pluggable Backend abstraction layer.

### Core Architecture Layers

1. **Backend Abstraction Layer** (`async_scheduler/backends/`)
   - `QueueBackend` - Task queue operations (enqueue/dequeue/peek/cancel/priority)
   - `LockBackend` - Distributed locking (acquire/release/extend/is_locked)
   - `CompletionDedupBackend` - Idempotent completion processing
   - `RegistryBackend` - Schedule registry for cron scheduling
   - Implementations: `InMemoryQueueBackend`, `InMemoryLockBackend`, `RedisQueueBackend`, `RedisLockBackend`

2. **Platform Services** (`async_scheduler/platform/`)
   - `TaskRouter` - Creates tasks and enqueues them (applies quota checks)
   - `TaskConsumer` - Background loop consuming from queue
   - `TaskCompletionNode` - Handles task completion with idempotency
   - `TaskReconciler` - Repairs stuck/orphaned tasks
   - `TenantQuotaManager` - Multi-tenant admission control
   - `CapabilityRegistry` - Registers and discovers task handlers

3. **DAG Engine** (`async_scheduler/dag/`)
   - `DAGEngine` - Orchestrates DAG execution with topological sorting
   - `StepExecutors` - Executes individual DAG steps with metrics
   - Supports conditional execution, retries, timeouts, skip/fallback

4. **Worker Layer** (`async_scheduler/worker/`)
   - `Worker` - Abstract base for task processors
   - `TaskWorker` - Pulls from queue and executes tasks
   - `WorkerPool` - Manages multiple workers in parallel

5. **Distributed Mode** (`async_scheduler/distributed/`)
   - `WorkerRegistry` - Redis-shaped worker liveness registry with heartbeat

### Task Lifecycle Flow

```
TaskRouter.create_task()
  → TaskRepository.create() (SQLite)
  → QueueManager.enqueue() (Backend)
  → TaskConsumer (background loop)
  → TaskExecutor.execute() (with timeout/retry)
  → TaskCompletionNode.process() (idempotent)
  → CallbackDispatcher (optional callbacks)
```

### Distributed Execution Attempt Flow

In distributed mode with Redis backends:
```
TaskConsumer.dequeue()
  → Create ExecutionAttempt with lease_token
  → WorkerRegistry.register()
  → LockBackend.acquire() (distributed lease)
  → Background heartbeat (extend lease)
  → TaskExecutor.execute()
  → CompletionDedupBackend.claim_once() (dedupe)
  → TaskReconciler (repairs stuck attempts)
```

### Service Container Pattern

Use `build_service_container()` to wire all components:
```python
from async_scheduler.platform import build_service_container
from async_scheduler.backends import BackendConfig

# Default in-memory mode
services = await build_service_container()

# Distributed mode with Redis
config = BackendConfig(
    distributed_mode=True,
    queue_type="redis",
    lock_type="redis",
    redis_url="redis://localhost:6379/0",
    lease_ttl_seconds=30,
    heartbeat_interval_seconds=10,
)
services = await build_service_container(backend_config=config)

# Access components
await services.task_consumer.start()
await services.cron_scheduler.start()
await services.worker_pool.start()
```

### Real Redis Transition State

The framework is in **transition state toward Redis-backed distributed semantics**. The following components support real async Redis client injection paths:

- `RedisQueueBackend` - Queue operations with Redis
- `RedisLockBackend` - Distributed locks with Redis
- `RedisCompletionDedupBackend` - Completion deduplication
- `WorkerRegistry` - Worker liveness with heartbeat

These are **not yet production-ready** (Lua/CAS atomicity pending), but they have comprehensive test coverage including:
- Single component real-Redis tests (`test_real_redis_*.py`)
- Shared-client integration tests (`tests/integration/test_real_redis_*.py`)
- Recovery/invariant tests

### Backend Factory

Components are created via `BackendFactory`:
```python
from async_scheduler.backends import BackendFactory, BackendConfig

config = BackendConfig(
    queue_type="redis" | "memory",
    lock_type="redis" | "memory",
    registry_type="memory",
    redis_url="...",
    lease_ttl_seconds=30,
)

factory = BackendFactory(config)
queue_backend = factory.create_queue_backend()
lock_backend = factory.create_lock_backend()
registry_backend = factory.create_registry_backend()
completion_backend = factory.create_completion_dedup_backend()
```

### Key Models

- `Task` - Core task entity with status (PENDING/QUEUED/RUNNING/SUCCESS/FAILED/CANCELLED/TIMEOUT/RETRY)
- `ExecutionAttempt` - Distributed execution attempt with lease token and heartbeat
- `DAG` - Workflow with nodes and dependencies
- `Schedule` - Cron-based task scheduling
- `Tenant` - Multi-tenancy with quota configuration

### Testing Patterns

- Use `fakeredis` for Redis backend tests without live Redis
- Use `get_session_no_context()` for tests that don't need async context
- Integration tests in `tests/integration/` test multi-worker coordination
- Real-Redis tests are prefixed with `test_real_redis_` and require Redis connection

### Distributed Mode Configuration

When `distributed_mode=True` in BackendConfig:
- Queue and lock backends use Redis implementations
- `WorkerRegistry` is created with Redis client
- `TaskConsumer` uses `LockBackend` for distributed leasing
- Execution attempts are tracked with `lease_token` for ownership

### DeepWiki Alignment

This framework aligns with deepwiki's distributed architecture:
- Similar component decomposition (Router/Queue/Consumer/Executor)
- Distributed locking and worker heartbeats
- Execution attempt tracking with leases
- Completion idempotency via dedup backend
- Reconciler for stuck task repair

Not yet implemented:
- Ray/ActorPoolManager
- Full Lua atomic operations
- ResourceManager/NodeAgent
- Async Proxy sidecar