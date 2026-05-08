# async-scheduler-runtime-core

Shared runtime/core implementation layer for independently packaged async-scheduler roles.

This package is not intended as an end-user-facing role by itself.
It provides shared bootstrapping and reusable runtime implementation that can be used by:
- `async-scheduler-control-plane`
- future `async-scheduler-task-api`
- future `async-scheduler-ops-api`
- future `async-scheduler-agent`

Current scope:
- bootstrap namespace for Plan B package independence
- initial control-plane runtime integration target
