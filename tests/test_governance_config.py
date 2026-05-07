import pytest
from src.services.governance import GovernanceService


def test_governance_default_high_risk_ops():
    svc = GovernanceService()
    assert svc.is_high_risk_operation("force_lease_eviction") is True
    assert svc.is_high_risk_operation("ordinary_replay") is False


def test_governance_config_override_high_risk_ops():
    svc = GovernanceService(config={"high_risk_operations": ["custom_risky_op"]})
    assert svc.is_high_risk_operation("custom_risky_op") is True
    assert svc.is_high_risk_operation("force_lease_eviction") is False


def test_governance_empty_config_uses_default():
    svc = GovernanceService(config={})
    assert svc.is_high_risk_operation("force_lease_eviction") is True


def test_force_lease_eviction_in_default_ops():
    svc = GovernanceService()
    assert svc.allowed_role_categories("force_lease_eviction") == ["admin"]
