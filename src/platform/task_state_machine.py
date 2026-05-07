"""Explicit task lifecycle state machine.

P1 goal: centralize legal task status transitions so live execution and repair
paths share the same rules.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from src.models.task import TaskStatus


class TaskEvent(str, Enum):
    CREATE = "create"
    SCHEDULE = "schedule"
    ENQUEUE = "enqueue"
    START = "start"
    COMPLETE = "complete"
    FAIL = "fail"
    CANCEL = "cancel"
    REQUEUE = "requeue"
    STALE_FAIL = "stale_fail"


@dataclass(frozen=True)
class TransitionResult:
    previous: TaskStatus
    event: TaskEvent
    current: TaskStatus


_ALLOWED: dict[tuple[TaskStatus, TaskEvent], TaskStatus] = {
    (TaskStatus.PENDING, TaskEvent.SCHEDULE): TaskStatus.SCHEDULED,
    (TaskStatus.PENDING, TaskEvent.ENQUEUE): TaskStatus.QUEUED,
    (TaskStatus.SCHEDULED, TaskEvent.ENQUEUE): TaskStatus.QUEUED,
    (TaskStatus.QUEUED, TaskEvent.START): TaskStatus.RUNNING,
    (TaskStatus.RUNNING, TaskEvent.COMPLETE): TaskStatus.COMPLETED,
    (TaskStatus.RUNNING, TaskEvent.FAIL): TaskStatus.FAILED,
    (TaskStatus.RUNNING, TaskEvent.STALE_FAIL): TaskStatus.FAILED,
    (TaskStatus.RUNNING, TaskEvent.REQUEUE): TaskStatus.QUEUED,
    (TaskStatus.QUEUED, TaskEvent.CANCEL): TaskStatus.CANCELLED,
}


class InvalidTaskTransition(ValueError):
    pass


class TaskStateMachine:
    @staticmethod
    def transition(current: TaskStatus, event: TaskEvent) -> TransitionResult:
        nxt = _ALLOWED.get((current, event))
        if nxt is None:
            raise InvalidTaskTransition(f"illegal task transition: {current.value} --{event.value}--> ?")
        return TransitionResult(previous=current, event=event, current=nxt)
