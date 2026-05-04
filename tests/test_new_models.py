"""Smoke tests for new src/models/ layer."""
from __future__ import annotations

import pytest

from src.models.task import (
    TaskCreate, TaskInfo, TaskStatus, TaskPriority,
    TaskCreateResponse, TaskSummary, TaskCancelResponse,
)
from src.models.dag import (
    DagStep, DagDefinition, DagContext, DagStatus, StepKind,
    OnFailureAction, RetryPolicy, FallbackConfig,
)
from src.models.capability import (
    CapabilityInfo, ActorConfig, CapabilityType, HealthStatus,
)
from src.models.cluster import (
    ClusterInfo, ClusterResources, ClusterStatus, PlannedResources, ObservedResources,
)
from src.models.node import (
    NodeInfo, NodeResources, NodeState, NodeLease, GPUInfo,
    InvitationRequest, InvitationResponse,
)
from src.models.schedule import ScheduleCreate, ScheduleInfo
from src.models.tenant import TenantInfo, TenantQuota, TenantStatus
from src.models.deploy import DeployedPackageInfo, DeployState, DeployRequest


# ---------------------------------------------------------------------------
# Task models
# ---------------------------------------------------------------------------

def test_task_create_basic():
    t = TaskCreate(task_type="inference", input_data={"prompt": "hello"})
    assert t.priority == TaskPriority.NORMAL
    assert t.tenant_id == ""


def test_task_status_values():
    assert TaskStatus.COMPLETED.value == "completed"
    assert TaskStatus.SCHEDULED.value == "scheduled"
    assert TaskStatus.QUEUED.value == "queued"
    assert TaskStatus.CANCELLED.value == "cancelled"


def test_task_priority_weight():
    assert TaskPriority.VERY_HIGH.weight == 1
    assert TaskPriority.HIGH.weight == 2
    assert TaskPriority.NORMAL.weight == 3
    assert TaskPriority.LOW.weight == 4
    assert TaskPriority.TIDE.weight == 5


def test_task_create_with_artifact():
    t = TaskCreate(
        task_type="ml",
        artifact_url="http://example.com/pkg.tar.gz",
        artifact_sha256="a" * 64,
    )
    assert t.artifact_sha256 == "a" * 64


def test_task_create_bad_artifact_sha256():
    with pytest.raises(Exception):
        TaskCreate(
            task_type="ml",
            artifact_url="http://example.com/pkg.tar.gz",
            artifact_sha256="tooshort",
        )


def test_task_info_lua_fix():
    info = TaskInfo.model_validate({
        "task_id": "t1",
        "task_type": "x",
        "input_data": [],     # Lua cjson empty list
        "output_data": [],
        "metadata_json": [],
    })
    assert info.input_data == {}
    assert info.output_data == {}


def test_task_create_response():
    r = TaskCreateResponse(
        task_id="t1", status=TaskStatus.PENDING,
    )
    assert r.idempotent_reused is False
    assert r.cluster_id == ""


def test_task_summary():
    s = TaskSummary(task_id="t1")
    assert s.status == TaskStatus.PENDING


def test_task_cancel_response():
    r = TaskCancelResponse(
        task_id="t1", cancelled=True, prior_status=TaskStatus.RUNNING
    )
    assert r.cancelled is True


# ---------------------------------------------------------------------------
# DAG models
# ---------------------------------------------------------------------------

def test_dag_step_defaults():
    step = DagStep(step_name="extract", capability="default")
    assert step.step_kind == StepKind.TASK
    assert step.depends_on == []
    assert step.on_failure == OnFailureAction.ABORT
    assert step.retry_policy.max_retries == 0


def test_dag_step_with_fallback():
    step = DagStep(
        step_name="flaky",
        capability="ml",
        on_failure=OnFailureAction.FALLBACK,
        fallback=FallbackConfig(output_mapping={"result": "default_result"}),
    )
    assert step.fallback is not None


def test_dag_definition_defaults():
    d = DagDefinition(dag_id="dag-1", steps=[])
    assert d.version == "1.0"
    assert d.enabled is True
    assert d.timeout_seconds == 3600


def test_dag_context():
    ctx = DagContext(task_id="t1", dag_id="d1")
    assert ctx.status == DagStatus.PENDING
    assert ctx.completed_steps == []
    assert ctx.step_results == {}


# ---------------------------------------------------------------------------
# Capability models
# ---------------------------------------------------------------------------

def test_capability_info_defaults():
    cap = CapabilityInfo(capability_name="inference")
    assert cap.capability_type == CapabilityType.RAY_ACTOR
    assert cap.health_status == HealthStatus.HEALTHY
    assert cap.version == "1.0.0"


def test_actor_config_defaults():
    ac = ActorConfig()
    assert ac.num_actors == 1
    assert ac.elastic_enabled is True
    assert ac.warmup_payload == {"_warmup": True}


def test_actor_config_artifact_validation():
    with pytest.raises(Exception):
        # artifact_url set but entrypoint missing
        ActorConfig(artifact_url="http://example.com/pkg.tar.gz")


def test_actor_config_with_valid_artifact():
    ac = ActorConfig(
        artifact_url="http://example.com/pkg.tar.gz",
        artifact_sha256="b" * 64,
        entrypoint="mypackage.module:MyClass",
    )
    assert ac.entrypoint == "mypackage.module:MyClass"


# ---------------------------------------------------------------------------
# Cluster models
# ---------------------------------------------------------------------------

def test_cluster_resources_flat_construction():
    cr = ClusterResources(fixed_gpus=4, tidal_gpus=2, total_gpus=6, available_gpus=4)
    assert cr.fixed_gpus == 4
    assert cr.tidal_gpus == 2
    assert cr.total_gpus == 6
    assert cr.available_gpus == 4


def test_cluster_resources_proxy():
    cr = ClusterResources(total_cpus=32, available_cpus=16, total_memory_gb=256.0)
    assert cr.total_cpus == 32
    assert cr.total_memory_gb == 256.0


def test_cluster_resources_serialise():
    cr = ClusterResources(fixed_gpus=4, total_gpus=6)
    d = cr.model_dump()
    assert d["fixed_gpus"] == 4
    assert d["total_gpus"] == 6


def test_cluster_info_defaults():
    c = ClusterInfo(cluster_id="c1")
    assert c.status == ClusterStatus.ACTIVE
    assert c.ray_head_address == ""
    assert c.capabilities == []


# ---------------------------------------------------------------------------
# Node models
# ---------------------------------------------------------------------------

def test_node_info_agent_url():
    n = NodeInfo(node_id="n1", host="10.0.0.1", agent_port=9100)
    assert n.agent_url == "http://10.0.0.1:9100"
    assert n.state == NodeState.IDLE


def test_node_resources_effective_no_ray():
    r = NodeResources(total_cpus=8, available_cpus=4, total_gpus=2, available_gpus=1)
    assert r.get_effective_cpus() == 8
    assert r.get_effective_gpus() == 2
    assert r.is_ray_synced is False


def test_node_resources_effective_with_ray():
    r = NodeResources(
        total_cpus=8,
        total_gpus=2,
        ray_sync_time=1234567890.0,
        ray_resources_total={"CPU": 16.0, "GPU": 4.0, "memory": 64 * 1024 * 1024 * 1024},
        ray_resources_available={"CPU": 8.0, "GPU": 2.0},
    )
    assert r.is_ray_synced is True
    assert r.get_effective_cpus() == 16
    assert r.get_effective_gpus() == 4


def test_node_lease():
    lease = NodeLease(node_id="n1", cluster_id="c1")
    assert lease.state == NodeState.RESERVED


def test_invitation_request():
    inv = InvitationRequest(cluster_id="c1", ray_head_address="192.168.1.1:6379")
    assert inv.labels == {}


# ---------------------------------------------------------------------------
# Schedule models
# ---------------------------------------------------------------------------

def test_schedule_create():
    s = ScheduleCreate(cron_expr="0 9 * * *", task_type="daily_job")
    assert s.enabled is True
    assert s.priority == "normal"


def test_schedule_info_lua_fix():
    info = ScheduleInfo.model_validate({
        "schedule_id": "s1",
        "cron_expr": "0 * * * *",
        "input_data": [],
        "metadata": [],
    })
    assert info.input_data == {}
    assert info.metadata == {}


# ---------------------------------------------------------------------------
# Tenant models
# ---------------------------------------------------------------------------

def test_tenant_info_auto_id():
    t = TenantInfo()
    assert t.tenant_id.startswith("tenant-")
    assert t.status == TenantStatus.ACTIVE
    assert t.quota.max_gpus == 0


def test_tenant_quota_lua_fix():
    q = TenantQuota.model_validate({
        "max_gpus": 0,
        "max_queue_depth": [],   # Lua empty list
        "max_concurrent_tasks": 0,
        "max_actor_count": [],   # Lua empty list
    })
    assert q.max_queue_depth == 0
    assert q.max_actor_count == 0


# ---------------------------------------------------------------------------
# Deploy models
# ---------------------------------------------------------------------------

def test_deploy_request():
    r = DeployRequest(package_url="http://x/pkg.tar.gz", package_name="mypkg")
    assert r.force is False
    assert r.setup_timeout_seconds == 300


def test_deployed_package_info():
    d = DeployedPackageInfo(
        package_name="pkg",
        package_version="1.0",
        package_url="http://x",
        deploy_path="/opt/pkg",
        state=DeployState.DEPLOYED,
    )
    assert d.state == DeployState.DEPLOYED
    assert d.manifest == {}
