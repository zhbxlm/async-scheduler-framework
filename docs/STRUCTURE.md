# Project Structure

> Auto-generated on 2026-05-05 00:47

```
async-scheduler-framework/
├── config
│   ├── dags
│   │   ├── 01_pipeline_linear.yaml
│   │   ├── 02_pipeline_branch.yaml
│   │   ├── 03_pipeline_map.yaml
│   │   ├── 04_pipeline_streaming.yaml
│   │   └── 99_video_generation_kitchen_sink.yaml
│   ├── __init__.py
│   ├── _background.py
│   ├── _dag.py
│   ├── _infra.py
│   ├── _scaling.py
│   ├── _task.py
│   ├── _tenant.py
│   ├── service_defaults.yaml
│   ├── settings.py
│   ├── settings_compat.py
│   └── settings_pydantic.py
├── docs
│   ├── deployment
│   │   └── docker.md
│   ├── plans
│   │   ├── 2026-05-01-real-redis-integration-plan.md
│   │   ├── 2026-05-01-redis-distributed-kernel-design.md
│   │   ├── 2026-05-01-redis-distributed-kernel-plan.md
│   │   └── 2026-05-04-config-optimization-packaging-plan.md
│   ├── reference
│   │   ├── 2026-05-01-distributed-kernel-stage-summary.md
│   │   ├── 2026-05-04-config-optimization-packaging-summary.md
│   │   ├── deepwiki-alignment-status.md
│   │   ├── deepwiki-distributed-architecture-reference.md
│   │   ├── next-phase-gap-analysis.md
│   │   └── non-critical-shared-state-boundary.md
│   ├── runtime
│   │   └── distributed-deployment-guide.md
│   ├── API.md
│   ├── STRUCTURE.md
│   ├── configuration.md
│   ├── implementation_summary.json
│   ├── mariadb-redis-validation-summary.md
│   ├── openapi.json
│   ├── pr-description.md
│   └── testing.md
├── examples
│   ├── async_proxy_worker_example.py
│   ├── custom_worker_example.py
│   └── end_to_end_demo.md
├── scripts
│   ├── __init__.py
│   ├── build_images.sh
│   ├── dag_deploy_verify.py
│   ├── dag_streaming_verify.py
│   ├── generate_docs.py
│   ├── live_redis_smoke_test.py
│   ├── live_redis_suite.py
│   └── run_local_split.sh
├── src
│   ├── agent
│   │   ├── __init__.py
│   │   ├── agent.yaml.example
│   │   ├── config.py
│   │   ├── ray_manager.py
│   │   ├── resource_detector.py
│   │   └── server.py
│   ├── api
│   │   ├── routes
│   │   │   ├── __init__.py
│   │   │   ├── capabilities.py
│   │   │   ├── clusters.py
│   │   │   ├── dags.py
│   │   │   ├── health.py
│   │   │   ├── nodes.py
│   │   │   ├── ops.py
│   │   │   ├── schedules.py
│   │   │   ├── tasks.py
│   │   │   └── tenants.py
│   │   ├── __init__.py
│   │   ├── auth.py
│   │   └── dependencies.py
│   ├── cli
│   │   ├── commands
│   │   │   ├── __init__.py
│   │   │   ├── capability.py
│   │   │   ├── cluster.py
│   │   │   ├── dag.py
│   │   │   ├── deploy.py
│   │   │   ├── node.py
│   │   │   ├── queue.py
│   │   │   ├── schedule.py
│   │   │   ├── task.py
│   │   │   ├── tenant.py
│   │   │   └── worker.py
│   │   ├── __init__.py
│   │   ├── client.py
│   │   └── main.py
│   ├── common
│   │   ├── __init__.py
│   │   ├── async_db.py
│   │   ├── container.py
│   │   ├── db.py
│   │   ├── error_handling.py
│   │   ├── lifecycle.py
│   │   ├── redis_client.py
│   │   └── tracing.py
│   ├── models
│   │   ├── __init__.py
│   │   ├── capability.py
│   │   ├── cluster.py
│   │   ├── dag.py
│   │   ├── deploy.py
│   │   ├── node.py
│   │   ├── schedule.py
│   │   ├── task.py
│   │   ├── tenant.py
│   │   └── tenant_context.py
│   ├── platform
│   │   ├── __init__.py
│   │   ├── base_registry.py
│   │   ├── capability_registry.py
│   │   ├── circuit_breaker.py
│   │   ├── cluster_registry.py
│   │   ├── cron_scheduler.py
│   │   ├── dag_engine.py
│   │   ├── dag_loader.py
│   │   ├── node_registry.py
│   │   ├── queue_keys.py
│   │   ├── queue_manager.py
│   │   ├── quota_enforcer.py
│   │   ├── raydata_client.py
│   │   ├── registry_interface.py
│   │   ├── remote_code_fetcher.py
│   │   ├── resource_manager.py
│   │   ├── schedule_registry.py
│   │   ├── step_executors.py
│   │   ├── task_completion_node.py
│   │   ├── task_consumer.py
│   │   ├── task_creator.py
│   │   ├── task_executor.py
│   │   ├── task_reconciler.py
│   │   ├── task_router.py
│   │   └── tenant_registry.py
│   ├── proxy
│   │   ├── __init__.py
│   │   ├── async_command_proxy.py
│   │   └── async_service_proxy.py
│   ├── workload
│   │   ├── __init__.py
│   │   ├── actor_pool_manager.py
│   │   ├── async_proxy_worker.py
│   │   ├── base_worker_actor.py
│   │   ├── scheduler_actor.py
│   │   └── worker_dev_kit.py
│   ├── __init__.py
│   ├── main.py
│   └── main_task_api.py
├── tests
│   ├── fixtures
│   │   └── long_running_worker.py
│   ├── __init__.py
│   ├── conftest.py
│   ├── fake_redis.py
│   ├── test_async_proxy.py
│   ├── test_async_proxy_callback_recent_summary_api.py
│   ├── test_async_proxy_callback_summary_api.py
│   ├── test_async_proxy_event_summary_api.py
│   ├── test_async_proxy_worker.py
│   ├── test_benchmarks.py
│   ├── test_build_images_script.sh
│   ├── test_callback_control_plane_summary_api.py
│   ├── test_callback_stats_api.py
│   ├── test_health_api.py
│   ├── test_new_final_batch.py
│   ├── test_new_models.py
│   ├── test_new_platform_components.py
│   ├── test_new_platform_layer.py
│   ├── test_new_workload_components.py
│   ├── test_quota_api_summary.py
│   ├── test_quota_enforcer.py
│   ├── test_resources_stats_workload_alignment.py
│   ├── test_split_script.sh
│   ├── test_task_creator.py
│   └── test_task_routes.py
├── Dockerfile
├── README.md
├── docker-compose.yml
├── pyproject.toml
├── requirements.txt
└── setup.py
```

## Module Descriptions

- **`src/api/`** — FastAPI routers and dependency injection
- **`src/api/routes/`** — Individual route handlers per resource
- **`src/common/`** — Shared utilities: DB, Redis, error handling, tracing, lifecycle
- **`src/models/`** — Pydantic data models for tasks, nodes, clusters, etc.
- **`src/platform/`** — Domain services: registries, scheduler, reconciler, queue
- **`src/workload/`** — Ray workload execution and worker actors
- **`src/cli/`** — Command-line interface (Click)
- **`src/agent/`** — Node agent: heartbeat, ownership, server
- **`config/`** — Application configuration (pydantic-settings based)
- **`tests/`** — Test suite: unit, integration, benchmarks
- **`.github/workflows/`** — CI/CD pipelines (GitHub Actions)
- **`docs/`** — Documentation (auto-generated and manual)