import pytest

from src.services.governance import GovernanceService


def test_is_high_risk_operation():
    svc = GovernanceService()
    assert svc.is_high_risk_operation("force_replay_on_active_lease") is True
    assert svc.is_high_risk_operation("force_replay_on_running_task") is True
    assert svc.is_high_risk_operation("force_replay_without_reason") is True
    assert svc.is_high_risk_operation("force_acknowledge_dead_letter_without_review") is True
    assert svc.is_high_risk_operation("force_lease_eviction") is True
    assert svc.is_high_risk_operation("ordinary_replay") is False


def test_requires_strong_audit():
    svc = GovernanceService()
    assert svc.requires_strong_audit("force_replay_on_active_lease") is True
    assert svc.requires_strong_audit("ordinary_replay") is False


def test_allowed_role_categories():
    svc = GovernanceService()
    assert svc.allowed_role_categories("force_replay_on_active_lease") == ["admin"]
    assert svc.allowed_role_categories("ordinary_replay") == ["operator", "admin"]
