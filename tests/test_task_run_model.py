from src.models.task_run import TaskRunRecord


def test_task_run_model_has_execution_instance_fields():
    cols = TaskRunRecord.__table__.columns.keys()
    assert "task_id" in cols
    assert "run_key" in cols
    assert "attempt" in cols
    assert "status" in cols
