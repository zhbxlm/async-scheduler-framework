"""Repository layer for database operations."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from async_scheduler.core.models import (
    DAG,
    DAGCreate,
    ExecutionAttempt,
    ExecutionAttemptCreate,
    ExecutionAttemptStatus,
    Schedule,
    ScheduleCreate,
    Task,
    TaskCreate,
    TaskStatus,
)
from async_scheduler.persistence.models import (
    DAGExecutionORM,
    DAGNodeORM,
    ExecutionAttemptORM,
    ScheduleORM,
    TaskORM,
)


class TaskRepository:
    """Repository for Task operations."""

    @staticmethod
    async def create(session: AsyncSession, task: TaskCreate) -> Task:
        if task.idempotency_key:
            existing = await TaskRepository.get_by_idempotency_key(
                session,
                task.idempotency_key,
                tenant_id=task.tenant_id,
            )
            if existing:
                return existing

        task_dict = task.model_dump()
        task_dict["id"] = str(uuid.uuid4())
        task_dict["status"] = TaskStatus.PENDING
        db_task = TaskORM(**task_dict)
        session.add(db_task)
        await session.flush()
        return Task.model_validate(db_task)

    @staticmethod
    async def get(session: AsyncSession, task_id: str) -> Task | None:
        result = await session.execute(select(TaskORM).where(TaskORM.id == task_id))
        db_task = result.scalar_one_or_none()
        return Task.model_validate(db_task) if db_task else None

    @staticmethod
    async def get_by_idempotency_key(
        session: AsyncSession,
        idempotency_key: str,
        tenant_id: str | None = None,
    ) -> Task | None:
        query = select(TaskORM).where(TaskORM.idempotency_key == idempotency_key)
        if tenant_id is None:
            query = query.where(TaskORM.tenant_id.is_(None))
        else:
            query = query.where(TaskORM.tenant_id == tenant_id)
        result = await session.execute(query.order_by(TaskORM.created_at.desc()))
        row = result.scalars().first()
        return Task.model_validate(row) if row else None

    @staticmethod
    async def list_all(
        session: AsyncSession,
        status: TaskStatus | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Task]:
        query = select(TaskORM)
        if status:
            query = query.where(TaskORM.status == status)
        query = query.order_by(TaskORM.created_at.desc()).limit(limit).offset(offset)
        result = await session.execute(query)
        return [Task.model_validate(row) for row in result.scalars().all()]

    @staticmethod
    async def update(session: AsyncSession, task_id: str, **kwargs: Any) -> Task | None:
        values = {}
        for key, value in kwargs.items():
            if value is not None:
                values[key] = value

        if not values:
            return await TaskRepository.get(session, task_id)

        values["updated_at"] = datetime.utcnow()

        result = await session.execute(
            update(TaskORM).where(TaskORM.id == task_id).values(**values).returning(TaskORM)
        )
        db_task = result.scalar_one_or_none()
        return Task.model_validate(db_task) if db_task else None

    @staticmethod
    async def delete(session: AsyncSession, task_id: str) -> bool:
        result = await session.execute(delete(TaskORM).where(TaskORM.id == task_id))
        return result.rowcount > 0

    @staticmethod
    async def get_next_queued(session: AsyncSession, limit: int = 10) -> list[Task]:
        query = (
            select(TaskORM)
            .where(TaskORM.status == TaskStatus.QUEUED)
            .where((TaskORM.scheduled_at.is_(None)) | (TaskORM.scheduled_at <= datetime.utcnow()))
            .order_by(TaskORM.priority.desc(), TaskORM.created_at.asc())
            .limit(limit)
        )
        result = await session.execute(query)
        return [Task.model_validate(row) for row in result.scalars().all()]


class ExecutionAttemptRepository:
    """Repository for distributed execution-attempt operations."""

    @staticmethod
    async def create(session: AsyncSession, attempt: ExecutionAttemptCreate) -> ExecutionAttempt:
        attempt_dict = attempt.model_dump()
        attempt_dict["id"] = str(uuid.uuid4())
        attempt_dict["status"] = ExecutionAttemptStatus.CLAIMED
        db_attempt = ExecutionAttemptORM(**attempt_dict)
        session.add(db_attempt)
        await session.flush()
        return ExecutionAttempt.model_validate(db_attempt)

    @staticmethod
    async def get(session: AsyncSession, attempt_id: str) -> ExecutionAttempt | None:
        result = await session.execute(select(ExecutionAttemptORM).where(ExecutionAttemptORM.id == attempt_id))
        db_attempt = result.scalar_one_or_none()
        return ExecutionAttempt.model_validate(db_attempt) if db_attempt else None

    @staticmethod
    async def update(session: AsyncSession, attempt_id: str, **kwargs: Any) -> ExecutionAttempt | None:
        values = {k: v for k, v in kwargs.items() if v is not None}
        if not values:
            return await ExecutionAttemptRepository.get(session, attempt_id)
        values["updated_at"] = datetime.utcnow()

        result = await session.execute(
            update(ExecutionAttemptORM)
            .where(ExecutionAttemptORM.id == attempt_id)
            .values(**values)
            .returning(ExecutionAttemptORM)
        )
        db_attempt = result.scalar_one_or_none()
        return ExecutionAttempt.model_validate(db_attempt) if db_attempt else None

    @staticmethod
    async def finalize(
        session: AsyncSession,
        attempt_id: str,
        status: ExecutionAttemptStatus,
        completed_at: datetime | None = None,
        error_message: str | None = None,
        result_payload: dict[str, Any] | None = None,
    ) -> ExecutionAttempt | None:
        return await ExecutionAttemptRepository.update(
            session,
            attempt_id,
            status=status,
            completed_at=completed_at or datetime.utcnow(),
            error_message=error_message,
            result_payload=result_payload,
        )

    @staticmethod
    async def get_latest_for_task(session: AsyncSession, task_id: str) -> ExecutionAttempt | None:
        result = await session.execute(
            select(ExecutionAttemptORM)
            .where(ExecutionAttemptORM.task_id == task_id)
            .order_by(ExecutionAttemptORM.created_at.desc())
            .limit(1)
        )
        db_attempt = result.scalar_one_or_none()
        return ExecutionAttempt.model_validate(db_attempt) if db_attempt else None

    @staticmethod
    async def list_for_task(
        session: AsyncSession,
        task_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ExecutionAttempt]:
        result = await session.execute(
            select(ExecutionAttemptORM)
            .where(ExecutionAttemptORM.task_id == task_id)
            .order_by(ExecutionAttemptORM.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        return [ExecutionAttempt.model_validate(row) for row in result.scalars().all()]

    @staticmethod
    async def list_latest_attempts(
        session: AsyncSession,
        limit: int = 100,
        offset: int = 0,
        worker_id: str | None = None,
    ) -> list[ExecutionAttempt]:
        latest_created_subquery = (
            select(
                ExecutionAttemptORM.task_id.label("task_id"),
                func.max(ExecutionAttemptORM.created_at).label("latest_created_at"),
            )
            .group_by(ExecutionAttemptORM.task_id)
            .subquery()
        )

        query = (
            select(ExecutionAttemptORM)
            .join(
                latest_created_subquery,
                (ExecutionAttemptORM.task_id == latest_created_subquery.c.task_id)
                & (ExecutionAttemptORM.created_at == latest_created_subquery.c.latest_created_at),
            )
            .order_by(ExecutionAttemptORM.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        if worker_id is not None:
            query = query.where(ExecutionAttemptORM.worker_id == worker_id)

        result = await session.execute(query)
        return [ExecutionAttempt.model_validate(row) for row in result.scalars().all()]


class ScheduleRepository:
    """Repository for Schedule operations."""

    @staticmethod
    async def create(session: AsyncSession, schedule: ScheduleCreate) -> Schedule:
        schedule_dict = schedule.model_dump()
        schedule_dict["id"] = str(uuid.uuid4())
        from async_scheduler.core.models import ScheduleStatus

        schedule_dict["status"] = ScheduleStatus.ACTIVE
        db_schedule = ScheduleORM(**schedule_dict)
        session.add(db_schedule)
        await session.flush()
        return Schedule.model_validate(db_schedule)

    @staticmethod
    async def get(session: AsyncSession, schedule_id: str) -> Schedule | None:
        result = await session.execute(select(ScheduleORM).where(ScheduleORM.id == schedule_id))
        db_schedule = result.scalar_one_or_none()
        return Schedule.model_validate(db_schedule) if db_schedule else None

    @staticmethod
    async def list_active(session: AsyncSession, limit: int = 100) -> list[Schedule]:
        from async_scheduler.core.models import ScheduleStatus

        query = (
            select(ScheduleORM)
            .where(ScheduleORM.status == ScheduleStatus.ACTIVE)
            .order_by(ScheduleORM.created_at.desc())
            .limit(limit)
        )
        result = await session.execute(query)
        return [Schedule.model_validate(row) for row in result.scalars().all()]

    @staticmethod
    async def update(session: AsyncSession, schedule_id: str, **kwargs: Any) -> Schedule | None:
        values = {k: v for k, v in kwargs.items() if v is not None}
        values["updated_at"] = datetime.utcnow()

        result = await session.execute(
            update(ScheduleORM).where(ScheduleORM.id == schedule_id).values(**values).returning(ScheduleORM)
        )
        db_schedule = result.scalar_one_or_none()
        return Schedule.model_validate(db_schedule) if db_schedule else None

    @staticmethod
    async def delete(session: AsyncSession, schedule_id: str) -> bool:
        result = await session.execute(delete(ScheduleORM).where(ScheduleORM.id == schedule_id))
        return result.rowcount > 0


class DAGRepository:
    """Repository for DAG operations."""

    @staticmethod
    async def create(session: AsyncSession, dag: DAGCreate) -> DAG:
        dag_dict = dag.model_dump()
        nodes = dag_dict.pop("nodes", [])

        dag_dict["id"] = str(uuid.uuid4())
        from async_scheduler.core.models import DAGExecutionStatus

        dag_dict["status"] = DAGExecutionStatus.PENDING

        db_dag = DAGExecutionORM(**dag_dict)
        session.add(db_dag)
        await session.flush()

        for node in nodes:
            node_dict = node.copy() if isinstance(node, dict) else node.model_dump()
            node_dict["id"] = node_dict.get("id") or str(uuid.uuid4())
            node_dict["dag_id"] = db_dag.id
            db_node = DAGNodeORM(**node_dict)
            session.add(db_node)

        await session.flush()

        from async_scheduler.core.models import DAGNode

        node_models = [DAGNode(**n.model_dump()) for n in nodes]
        return DAG(
            id=db_dag.id,
            nodes=node_models,
            **{k: v for k, v in dag_dict.items() if k != "nodes"},
        )

    @staticmethod
    async def get(session: AsyncSession, dag_id: str) -> DAG | None:
        result = await session.execute(select(DAGExecutionORM).where(DAGExecutionORM.id == dag_id))
        db_dag = result.scalar_one_or_none()

        if not db_dag:
            return None

        nodes_result = await session.execute(select(DAGNodeORM).where(DAGNodeORM.dag_id == dag_id))
        from async_scheduler.core.models import DAGNode

        nodes = [DAGNode.model_validate(n) for n in nodes_result.scalars().all()]

        return DAG(
            id=db_dag.id,
            name=db_dag.name,
            description=db_dag.description,
            nodes=nodes,
            tenant_id=db_dag.tenant_id,
            max_parallelism=db_dag.max_parallelism,
            status=db_dag.status,
            created_at=db_dag.created_at,
            updated_at=db_dag.updated_at,
            started_at=db_dag.started_at,
            completed_at=db_dag.completed_at,
            node_executions={k: v for k, v in db_dag.node_executions.items()},
            context=db_dag.context,
        )

    @staticmethod
    async def update(session: AsyncSession, dag_id: str, **kwargs: Any) -> DAG | None:
        values = {k: v for k, v in kwargs.items() if v is not None}
        values["updated_at"] = datetime.utcnow()

        result = await session.execute(
            update(DAGExecutionORM).where(DAGExecutionORM.id == dag_id).values(**values).returning(DAGExecutionORM)
        )
        db_dag = result.scalar_one_or_none()
        if db_dag:
            return await DAGRepository.get(session, dag_id)
        return None

    @staticmethod
    async def list_all(session: AsyncSession, limit: int = 100) -> list[DAG]:
        query = select(DAGExecutionORM).order_by(DAGExecutionORM.created_at.desc()).limit(limit)
        result = await session.execute(query)
        dag_ids = [row.id for row in result.scalars().all()]
        return [await DAGRepository.get(session, dag_id) for dag_id in dag_ids if dag_id]
