from src.models.task import TaskStatus
from src.platform.task_state_machine import TaskStateMachine, TaskEvent, InvalidTaskTransition


def test_pending_to_queued_allowed():
    result = TaskStateMachine.transition(TaskStatus.PENDING, TaskEvent.ENQUEUE)
    assert result.current == TaskStatus.QUEUED


def test_queued_to_running_allowed():
    result = TaskStateMachine.transition(TaskStatus.QUEUED, TaskEvent.START)
    assert result.current == TaskStatus.RUNNING


def test_running_to_completed_allowed():
    result = TaskStateMachine.transition(TaskStatus.RUNNING, TaskEvent.COMPLETE)
    assert result.current == TaskStatus.COMPLETED


def test_running_to_failed_allowed():
    result = TaskStateMachine.transition(TaskStatus.RUNNING, TaskEvent.FAIL)
    assert result.current == TaskStatus.FAILED


def test_queued_to_cancelled_allowed():
    result = TaskStateMachine.transition(TaskStatus.QUEUED, TaskEvent.CANCEL)
    assert result.current == TaskStatus.CANCELLED


def test_running_to_cancelled_forbidden():
    try:
        TaskStateMachine.transition(TaskStatus.RUNNING, TaskEvent.CANCEL)
        assert False, "expected InvalidTaskTransition"
    except InvalidTaskTransition:
        assert True
