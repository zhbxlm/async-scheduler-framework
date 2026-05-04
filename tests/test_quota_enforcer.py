"""Tests for quota enforcer."""
import pytest
from src.platform.quota_enforcer import QuotaEnforcer


def test_quota_enforcer_initialization():
    enforcer = QuotaEnforcer()
    assert enforcer is not None
