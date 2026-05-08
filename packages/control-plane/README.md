# async-scheduler-control-plane

Standalone control-plane worker for [async-scheduler](https://github.com/zhbxlm/async-scheduler-framework).

Runs background loops outside task-api / ops-api, including:
- cron scheduling
- task reconciliation / repair
- compensation processing
- callback dispatch

## Install

```bash
pip install async-scheduler-control-plane
```

## Start

```bash
scheduler-control-plane
```

## Runtime role

This package is intended to be deployed alongside:
- `async-scheduler-task-api`
- `async-scheduler-ops-api`

It is not a public API service. It is a background worker/process role.
