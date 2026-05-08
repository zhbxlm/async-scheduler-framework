"""Tests for alert management system."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.monitoring.alerts import (
    Alert,
    AlertAnnotation,
    AlertChannel,
    AlertFormatter,
    AlertLabel,
    AlertNotifier,
    AlertRouter,
    AlertSeverity,
    AlertStatus,
    process_alertmanager_webhook,
)

# ── Fixtures ─────────────────────────────────────────────────────────────

def make_alert(
    alertname: str = "TestAlert",
    severity: AlertSeverity = AlertSeverity.WARNING,
    service: str = "task-api",
    status: AlertStatus = AlertStatus.FIRING,
    fingerprint: str = "fp-001",
) -> Alert:
    return Alert(
        status=status,
        labels=AlertLabel(alertname=alertname, severity=severity, service=service),
        annotations=AlertAnnotation(
            summary=f"{alertname} summary",
            description=f"{alertname} description",
            runbook_url="https://example.com/runbook",
        ),
        starts_at=datetime.now(timezone.utc),
        fingerprint=fingerprint,
    )


def make_webhook_payload(alerts: list | None = None) -> dict:
    alert = {
        "status": "firing",
        "labels": {
            "alertname": "ServiceDown",
            "severity": "critical",
            "service": "task-api",
            "instance": "task-api-1",
        },
        "annotations": {
            "summary": "task-api is down",
            "description": "Service down for >1m",
            "runbook_url": "https://example.com/runbook",
        },
        "startsAt": "2026-05-06T12:00:00+00:00",
        "endsAt": None,
        "generatorURL": "http://prometheus:9090/graph",
        "fingerprint": "abc123",
    }
    return {
        "version": "4",
        "groupKey": "{}:{alertname='ServiceDown'}",
        "truncatedAlerts": 0,
        "status": "firing",
        "receiver": "webhook",
        "groupLabels": {"alertname": "ServiceDown"},
        "commonLabels": {"alertname": "ServiceDown", "severity": "critical"},
        "commonAnnotations": {"summary": "task-api is down"},
        "externalURL": "http://alertmanager:9093",
        "alerts": alerts or [alert],
    }


# ── AlertLabel ────────────────────────────────────────────────────────────

class TestAlertLabel:
    def test_basic_label(self):
        label = AlertLabel(alertname="ServiceDown", severity=AlertSeverity.CRITICAL)
        assert label.alertname == "ServiceDown"
        assert label.severity == AlertSeverity.CRITICAL
        assert label.service is None

    def test_label_with_service(self):
        label = AlertLabel(
            alertname="QueueBacklogCritical",
            severity=AlertSeverity.WARNING,
            service="task-api",
            capability="gpu",
        )
        assert label.service == "task-api"
        assert label.capability == "gpu"


# ── AlertFormatter ────────────────────────────────────────────────────────

class TestAlertFormatter:
    def test_format_firing_alert_for_infoflow(self):
        alert = make_alert(severity=AlertSeverity.CRITICAL)
        result = AlertFormatter.format_for_infoflow(alert)

        assert "🔴" in result["message"]
        assert "告警触发" in result["message"]
        assert "TestAlert" in result["message"]
        assert "task-api" in result["message"]
        assert isinstance(result["mentions"], list)

    def test_format_resolved_alert_for_infoflow(self):
        alert = make_alert(status=AlertStatus.RESOLVED)
        alert.ends_at = datetime.now(timezone.utc)
        result = AlertFormatter.format_for_infoflow(alert)

        assert "告警恢复" in result["message"]
        assert "✅" in result["message"]

    def test_format_warning_emoji(self):
        alert = make_alert(severity=AlertSeverity.WARNING)
        result = AlertFormatter.format_for_infoflow(alert)
        assert "🟡" in result["message"]

    def test_format_info_emoji(self):
        alert = make_alert(severity=AlertSeverity.INFO)
        result = AlertFormatter.format_for_infoflow(alert)
        assert "🔵" in result["message"]

    def test_format_for_email(self):
        alert = make_alert(severity=AlertSeverity.CRITICAL)
        result = AlertFormatter.format_for_email(alert)

        assert "subject" in result
        assert "body" in result
        assert "CRITICAL" in result["subject"]
        assert "TestAlert" in result["subject"]
        assert "FIRING" in result["subject"]

    def test_format_for_webhook(self):
        alert = make_alert()
        result = AlertFormatter.format_for_webhook(alert)
        assert result["fingerprint"] == "fp-001"
        assert result["status"] == "firing"

    def test_critical_mentions_service_owners(self):
        alert = make_alert(severity=AlertSeverity.CRITICAL, service="task-api")
        result = AlertFormatter.format_for_infoflow(alert)
        # Critical task-api alerts should mention service owners
        assert len(result["mentions"]) > 0

    def test_warning_no_mandatory_mentions(self):
        alert = make_alert(severity=AlertSeverity.WARNING)
        result = AlertFormatter.format_for_infoflow(alert)
        # Warnings may have fewer or no mandatory mentions
        assert isinstance(result["mentions"], list)

    def test_runbook_url_included(self):
        alert = make_alert()
        result = AlertFormatter.format_for_infoflow(alert)
        assert "runbook" in result["message"].lower() or "处理手册" in result["message"]


# ── AlertRouter ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
class TestAlertRouter:
    async def test_critical_routes_to_infoflow_and_webhook(self):
        router = AlertRouter()
        alert = make_alert(severity=AlertSeverity.CRITICAL)
        channels = await router.route_alert(alert)

        assert AlertChannel.INFOFLOW in channels
        assert AlertChannel.WEBHOOK in channels

    async def test_warning_routes_to_infoflow(self):
        router = AlertRouter()
        alert = make_alert(severity=AlertSeverity.WARNING)
        channels = await router.route_alert(alert)

        assert AlertChannel.INFOFLOW in channels

    async def test_info_routes_to_webhook_only(self):
        router = AlertRouter()
        alert = make_alert(severity=AlertSeverity.INFO)
        channels = await router.route_alert(alert)

        assert AlertChannel.WEBHOOK in channels
        assert AlertChannel.INFOFLOW not in channels

    async def test_custom_route_added(self):
        router = AlertRouter()
        router.add_route({"service": "special-api"}, AlertChannel.EMAIL)
        alert = make_alert(severity=AlertSeverity.INFO, service="special-api")

        channels = await router.route_alert(alert)
        assert AlertChannel.EMAIL in channels

    async def test_custom_route_not_matched(self):
        router = AlertRouter()
        router.add_route({"service": "special-api"}, AlertChannel.EMAIL)
        alert = make_alert(severity=AlertSeverity.INFO, service="other-api")

        channels = await router.route_alert(alert)
        assert AlertChannel.EMAIL not in channels

    async def test_no_duplicate_channels(self):
        router = AlertRouter()
        router.add_route({"severity": "critical"}, AlertChannel.INFOFLOW)
        alert = make_alert(severity=AlertSeverity.CRITICAL)

        channels = await router.route_alert(alert)
        assert len(channels) == len(set(channels))


# ── AlertNotifier ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
class TestAlertNotifier:
    async def test_notify_calls_channels(self):
        router = AlertRouter()
        notifier = AlertNotifier(router)

        alert = make_alert(severity=AlertSeverity.CRITICAL)
        results = await notifier.notify(alert)

        assert isinstance(results, dict)
        # At least one channel should have been attempted
        assert len(results) > 0

    async def test_notify_returns_success_status(self):
        router = AlertRouter()
        notifier = AlertNotifier(router)
        alert = make_alert(severity=AlertSeverity.WARNING)

        results = await notifier.notify(alert)
        for channel, success in results.items():
            assert isinstance(success, bool)

    async def test_channel_failure_does_not_crash(self):
        """If one channel fails, others should still be notified."""
        router = AlertRouter()
        notifier = AlertNotifier(router)

        # Monkey-patch to simulate channel failure
        call_count = {"n": 0}

        async def flaky_send(channel, alert):
            call_count["n"] += 1
            if channel == AlertChannel.WEBHOOK:
                raise RuntimeError("Webhook unreachable")
            return True

        notifier._send_to_channel = flaky_send

        alert = make_alert(severity=AlertSeverity.CRITICAL)
        results = await notifier.notify(alert)

        # Should have results even if webhook failed
        assert isinstance(results, dict)
        assert call_count["n"] > 0


# ── process_alertmanager_webhook ──────────────────────────────────────────

@pytest.mark.asyncio
class TestProcessAlertManagerWebhook:
    async def test_process_valid_payload(self):
        payload = make_webhook_payload()
        result = await process_alertmanager_webhook(payload)

        assert result["status"] == "success"
        assert result["alerts_processed"] == 1

    async def test_process_multiple_alerts(self):
        alert1 = make_webhook_payload()["alerts"][0]
        alert2 = {**alert1, "fingerprint": "xyz789"}
        payload = make_webhook_payload(alerts=[alert1, alert2])

        result = await process_alertmanager_webhook(payload)
        assert result["alerts_processed"] == 2

    async def test_process_resolved_alert(self):
        payload = make_webhook_payload()
        payload["status"] = "resolved"
        payload["alerts"][0]["status"] = "resolved"
        payload["alerts"][0]["endsAt"] = "2026-05-06T12:05:00+00:00"

        result = await process_alertmanager_webhook(payload)
        assert result["status"] == "success"

    async def test_process_invalid_payload_returns_error(self):
        result = await process_alertmanager_webhook({"invalid": "data"})
        assert result["status"] == "error"

    async def test_results_contain_fingerprints(self):
        payload = make_webhook_payload()
        result = await process_alertmanager_webhook(payload)

        assert "results" in result
        assert "abc123" in result["results"]


# ── Metrics integration ───────────────────────────────────────────────────

class TestAlertMetricsIntegration:
    def test_update_redis_ha_metrics_no_crash(self):
        """update_redis_ha_metrics should not raise even with bad input."""
        from src.monitoring.metrics import update_redis_ha_metrics
        update_redis_ha_metrics("CLOSED", False, 0)
        update_redis_ha_metrics("OPEN", True, 42)
        update_redis_ha_metrics("HALF_OPEN", False, 0)

    def test_redis_ha_metrics_registered(self):
        from src.monitoring.metrics import (
            REDIS_CIRCUIT_BREAKER_STATE,
            REDIS_CIRCUIT_BREAKER_TRIPS,
            REDIS_DEGRADATION_MODE,
            REDIS_DEGRADATION_QUEUE_SIZE,
            REDIS_RETRY_TOTAL,
        )
        # Metrics objects should exist and be usable
        assert REDIS_CIRCUIT_BREAKER_STATE is not None
        assert REDIS_DEGRADATION_MODE is not None
        assert REDIS_DEGRADATION_QUEUE_SIZE is not None
        assert REDIS_RETRY_TOTAL is not None
        assert REDIS_CIRCUIT_BREAKER_TRIPS is not None
