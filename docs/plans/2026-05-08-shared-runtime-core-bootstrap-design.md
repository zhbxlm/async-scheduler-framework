# Shared Runtime Core Bootstrap Design

Date: 2026-05-08
Project: async-scheduler-framework
Status: proposed

## Goal

Create the smallest possible shared publishable runtime/core package skeleton to support Plan B migration, starting with control-plane only.

## Scope of this wave

This wave does **not** migrate all runtime logic into the new shared package.
It establishes a bootstrap-capable package boundary that lets control-plane stop depending directly on package-local bridge layers and prepares the repository for further extraction.

## Immediate target

Introduce a new package directory:
- `packages/runtime-core`

This package will initially provide:
- package metadata
- importable namespace
- minimal runtime-facing bootstrap module(s)
- a place to move shared runtime bootstrapping logic next

## Design principles

1. Keep first wave small.
2. Do not copy half the framework into the new package immediately.
3. Establish a valid package boundary and import namespace first.
4. Only support control-plane in this wave.

## Wave-1 contents

- `packages/runtime-core/pyproject.toml`
- `packages/runtime-core/README.md`
- `packages/runtime-core/src/scheduler_runtime_core/__init__.py`
- `packages/runtime-core/src/scheduler_runtime_core/control_plane.py`

`control_plane.py` will initially expose a shared entrypoint wrapper for packaged consumers.
This is a bootstrap milestone, not the final extraction endpoint.

## Why this is useful even before full extraction

Because it creates:
- a stable package name,
- a stable import namespace,
- a place to progressively move control-plane shared runtime code,
- a migration target for packaged roles.

## Next follow-up after this wave

1. move control-plane runtime bootstrap from package-local runtime into runtime-core,
2. replace remaining `src.*` imports in the packaged control-plane path,
3. then migrate task-api and ops-api using the same shared namespace.

## Success criteria

- runtime-core package builds successfully,
- build script includes it,
- documentation mentions it as shared runtime implementation layer,
- no current runtime behavior regression.
