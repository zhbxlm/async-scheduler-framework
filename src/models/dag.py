"""DAG domain models — aligned with docs/deepwiki-reference/数据模型.md"""
from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class ExecutionMode(str, Enum):
    SYNC = "sync"
    ASYNC = "async"
    FLASK_WRAPPED = "flask_wrapped"


class OnFailureAction(str, Enum):
    ABORT = "abort"
    SKIP = "skip"
    FALLBACK = "fallback"


class MapErrorPolicy(str, Enum):
    ABORT_ALL = "abort_all"
    CONTINUE = "continue"


class StepKind(str, Enum):
    TASK = "task"
    DISPATCH = "dispatch"
    COLLECTOR = "collector"
    MAP = "map"
    STREAMING = "streaming"


class DagStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# ---------------------------------------------------------------------------
# Config sub-models
# ---------------------------------------------------------------------------

class RetryPolicy(BaseModel):
    max_retries: int = 0
    retry_delay_seconds: float = 1.0


class CheckpointConfig(BaseModel):
    enabled: bool = False
    interval_steps: int = 10
    storage_path: str = "/shared/checkpoints"


class PollingConfig(BaseModel):
    interval_seconds: float = 5.0


class FlaskStepConfig(BaseModel):
    url: str = ""


class QueueStepConfig(BaseModel):
    max_queue_depth: int = 50
    max_concurrent: int = 4
    qps_limit: float | None = None


class FallbackConfig(BaseModel):
    output_mapping: dict[str, str] = Field(default_factory=dict)


class StreamingTrigger(BaseModel):
    buffer_key: str
    trigger_condition: str = "chunk_ready"
    flush_on_complete: bool = True
    downstream_steps: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Core models
# ---------------------------------------------------------------------------

class DagStep(BaseModel):
    """Single step definition in a DAG."""

    # Basic
    step_name: str
    capability: str
    step_kind: StepKind = StepKind.TASK

    # Dependencies
    depends_on: list[str] = Field(default_factory=list)
    collect_from: list[str] = Field(default_factory=list)

    # Execution
    execution_mode: ExecutionMode = ExecutionMode.SYNC
    timeout_seconds: int = Field(default=60, ge=1, le=86400)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)

    # Data mapping
    input_mapping: dict[str, str] = Field(default_factory=dict)
    output_mapping: dict[str, str] = Field(default_factory=dict)
    artifact_output_fields: dict[str, str] = Field(default_factory=dict)

    # Condition & fault-tolerance
    condition: str | None = None
    on_failure: OnFailureAction = OnFailureAction.ABORT
    fallback: FallbackConfig | None = None

    # Special configs
    checkpoint: CheckpointConfig | None = None
    polling: PollingConfig | None = None
    flask: FlaskStepConfig | None = None
    queue: QueueStepConfig | None = None
    map_over: str | None = None
    max_concurrency: int = Field(default=0, ge=0)
    map_error_policy: MapErrorPolicy = MapErrorPolicy.ABORT_ALL
    streaming_trigger: StreamingTrigger | None = None


class DagDefinition(BaseModel):
    """Complete DAG definition."""
    dag_id: str
    tenant_id: str = ""
    name: str = ""
    version: str = "1.0"
    timeout_seconds: int = 3600
    enabled: bool = True
    steps: list[DagStep] = Field(default_factory=list)
    max_queue_depth: int | None = None
    max_concurrent: int | None = None


class DagContext(BaseModel):
    """DAG execution context stored in Redis."""
    schema_version: int = 1  # bumped when DagContext fields change
    task_id: str
    dag_id: str
    tenant_id: str = ""
    input_data: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    context: dict[str, Any] = Field(default_factory=dict)
    step_results: dict[str, Any] = Field(default_factory=dict)
    completed_steps: list[str] = Field(default_factory=list)
    status: DagStatus = DagStatus.PENDING
