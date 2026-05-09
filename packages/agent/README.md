# async-scheduler-agent

Node agent package for async-scheduler worker deployment and lifecycle management.

## Runtime model

This package is currently a **thin launcher package**.

It provides:
- agent role identity
- CLI entrypoint
- package-local startup wiring

It does **not** yet contain the complete agent business/runtime implementation by itself.
In the current architecture it runs together with:
- shared runtime/bootstrap support from `async-scheduler-runtime-core`
- monorepo business/runtime implementation provided by the framework install

### Dependency semantics

- package `dependencies` = **source-level minimum dependencies**
- runnable deployment dependencies = use role-oriented runtime profiles once the agent runtime profile is selected for your deployment

## Install

### Minimal package install

```bash
pip install async-scheduler-agent
```

## Start

```bash
scheduler-agent start --scheduler-url http://localhost:8000
```
