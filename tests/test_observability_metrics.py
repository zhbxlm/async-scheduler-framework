from src.common.metrics import (
    DEAD_LETTER_EVENTS_TOTAL,
    FORCE_OPERATIONS_TOTAL,
    OPERATOR_ACTIONS_TOTAL,
    REPLAY_REQUESTS_TOTAL,
    STALE_TASKS_GAUGE,
)
from src.common.service_logger import log_service_event


def test_platform_metrics_importable():
    # Metrics should be importable without error even without prometheus installed
    assert REPLAY_REQUESTS_TOTAL is not None
    assert DEAD_LETTER_EVENTS_TOTAL is not None
    assert OPERATOR_ACTIONS_TOTAL is not None
    assert FORCE_OPERATIONS_TOTAL is not None
    assert STALE_TASKS_GAUGE is not None


def test_service_logger_emits_without_error(caplog):
    import logging
    with caplog.at_level(logging.INFO, logger="platform.service"):
        log_service_event("TestService", "test_op", task_id="t1", actor="alice", outcome="ok")
    assert any("test_op" in r.message for r in caplog.records)


def test_platform_metrics_inc_without_error():
    # Should not raise even when prometheus not installed (DummyMetric)
    REPLAY_REQUESTS_TOTAL.labels(actor_role="operator", allowed="True").inc()
    DEAD_LETTER_EVENTS_TOTAL.labels(event="replayed").inc()
    OPERATOR_ACTIONS_TOTAL.labels(action_type="task_replay_requested").inc()
    FORCE_OPERATIONS_TOTAL.labels(operation="force_lease_eviction", outcome="success").inc()
    STALE_TASKS_GAUGE.set(5)
