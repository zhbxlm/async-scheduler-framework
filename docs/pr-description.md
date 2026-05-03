# PR Description

## Title

Improve MySQL-family compatibility and validate distributed flows with local Redis + MariaDB

## Body

```markdown
## Summary

This PR moves the project further away from a SQLite-centric local assumption and toward a more realistic MySQL-family + Redis deployment model.

It also validates core distributed flows against a local **userland Redis + MariaDB** setup, under a constrained environment where Docker and system-level package installation were unavailable.

---

## What changed

### 1. Configuration and runtime binding
- added unified runtime settings via `async_scheduler/settings.py`
- added enhanced config module under `async_scheduler/config/`
- made persistence engine binding lazy / rebindable
- ensured test runs honor `TEST_DATABASE_URL`
- improved test-time DB isolation and engine reset behavior

### 2. Deployment artifacts
- added `Dockerfile`
- added `.dockerignore`
- added `docker-compose.yml`
- added `docker/entrypoint.sh`
- added `.env.example`
- added `scripts/build_images.sh`
- added `scripts/run_local_split.sh`
- added deployment/config/testing docs

### 3. MySQL / MariaDB compatibility fixes
- removed `RETURNING`-based repository update flow
- improved `drop_db()` behavior for MySQL-family databases
- stabilized latest execution-attempt ordering using:
  - `created_at desc`
  - `retry_index desc`
  - `id desc`
- adapted timestamp assertions for MariaDB precision behavior

### 4. Redis-backed distributed behavior fixes
- fixed Redis lock TTL precision:
  - second-based rounding caused sub-second leases to become `1s`
  - switched to millisecond precision (`px` / `pexpire`)
- improved lease-loss handling in distributed consumer flows
- improved stale lease / repair detection behavior
- isolated Redis state between tests via `flushdb()`

### 5. Test infrastructure improvements
- added `mysql_required` / `redis_required` markers
- added MySQL / Redis availability gating
- added per-test-file database isolation
- added validation summary doc for local Redis + MariaDB execution

---

## Validation environment

Local userland services were used instead of system-installed packages:

- **Redis**: `127.0.0.1:6379`
- **MariaDB**: `127.0.0.1:3307`

This was necessary because:
- Docker installation was unavailable in the current environment
- system-level MySQL / Redis installation was not allowed
- userland Redis + MariaDB were bootstrapped and used for verification

---

## Validated test suites

The following core suites were validated against local Redis + MariaDB:

- `tests/test_task_lifecycle.py`
- `tests/test_execution_attempts.py`
- `tests/test_observability_api.py`
- `tests/test_consumer_attempt_consistency.py`
- `tests/test_completion_idempotency.py`
- `tests/test_distributed_claim_flow.py`
- `tests/test_distributed_reconciler.py`

### Result
```text
44 passed
```

---

## Commit breakdown

This PR is intentionally split into 3 logical commits:

1. **refactor: unify runtime settings and database binding**
2. **feat: add Docker and local split deployment artifacts**
3. **fix: improve MySQL/MariaDB and Redis distributed compatibility**

---

## Notes

This PR does **not** claim full equivalence with official MySQL 8 yet.

However, it substantially improves compatibility across the **MySQL/MariaDB family**, removes several SQLite/PostgreSQL-oriented assumptions, and validates core distributed behavior using real local Redis + MariaDB processes rather than only in-memory/fake test paths.

A validation summary is included in:

- `docs/mariadb-redis-validation-summary.md`
```
