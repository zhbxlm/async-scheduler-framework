# API Design

## Two-Service Architecture

async-scheduler exposes two independent HTTP services:

| Service | Port | Audience | Package |
|---------|------|----------|---------|
| **Task API** | 8001 | End users / caller services | `async-scheduler-task-api` |
| **Ops API** | 8000 | Operators / admins | `async-scheduler-ops-api` |

Separating the services allows independent scaling, different network exposure (Task API public, Ops API internal-only), and distinct auth policies.

## Task API Design Principles

- REST over HTTP/1.1 with JSON bodies
- All task operations are idempotent on `task_id`
- Asynchronous by default — submit returns immediately, poll for status
- Callback URL for push notification on completion

## Ops API Design Principles

- Full CRUD on all configuration entities (clusters, nodes, DAGs, capabilities, tenants)
- Read-heavy: list endpoints support cursor pagination and rich filtering
- Write operations validated against business rules (e.g., capability name uniqueness)
