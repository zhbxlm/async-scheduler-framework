"""Tests for quota enforcer."""
import pytest
from src.platform.quota_enforcer import QuotaEnforcer


def test_quota_enforcer_initialization():
    from unittest.mock import MagicMock
    redis_mock = MagicMock()
    enforcer = QuotaEnforcer(redis_client=redis_mock)
    assert enforcer is not None
