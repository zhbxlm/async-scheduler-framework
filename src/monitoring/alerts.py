"""Alert management and webhook receiver for Prometheus Alertmanager.

This module provides:
- Webhook receiver for Alertmanager alerts
- Alert routing to multiple channels (Infoflow, email, webhook)
- Alert formatting and templating
- Alert history and state management
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, ConfigDict
from pydantic.alias_generators import to_camel

logger = logging.getLogger(__name__)


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class AlertStatus(str, Enum):
    """Alert status from Alertmanager."""
    FIRING = "firing"
    RESOLVED = "resolved"


class AlertLabel(BaseModel):
    """Alert labels from Prometheus."""
    alertname: str
    severity: AlertSeverity
    service: Optional[str] = None
    capability: Optional[str] = None
    tenant_id: Optional[str] = None
    instance: Optional[str] = None
    # Additional labels
    extra: Dict[str, str] = Field(default_factory=dict)


class AlertAnnotation(BaseModel):
    """Alert annotations from Prometheus."""
    summary: str
    description: str
    runbook_url: Optional[str] = None


class Alert(BaseModel):
    """Alert from Alertmanager webhook."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    status: AlertStatus
    labels: AlertLabel
    annotations: AlertAnnotation
    starts_at: datetime
    ends_at: Optional[datetime] = None
    generator_url: Optional[str] = None
    fingerprint: str


class AlertManagerWebhook(BaseModel):
    """Alertmanager webhook payload."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    version: str = "4"
    group_key: str
    truncated_alerts: int = 0
    status: AlertStatus
    receiver: str
    group_labels: Dict[str, str]
    common_labels: Dict[str, str]
    common_annotations: Dict[str, str]
    external_url: str = Field(alias="externalURL", default="")
    alerts: List[Alert]


class AlertChannel(str, Enum):
    """Alert notification channels."""
    INFOFLOW = "infoflow"
    EMAIL = "email"
    WEBHOOK = "webhook"
    SLACK = "slack"
    PAGERDUTY = "pagerduty"


class AlertRouter:
    """Route alerts to appropriate channels based on severity and service."""
    
    def __init__(self):
        self._channels: Dict[AlertChannel, Any] = {}
        self._routing_rules: List[Tuple[Dict[str, Any], AlertChannel]] = []
        
    def add_channel(self, channel: AlertChannel, config: Dict[str, Any]) -> None:
        """Add a notification channel."""
        self._channels[channel] = config
        logger.info("Added alert channel: %s", channel)
    
    def add_route(self, match: Dict[str, Any], channel: AlertChannel) -> None:
        """Add a routing rule."""
        self._routing_rules.append((match, channel))
        logger.info("Added routing rule: %s -> %s", match, channel)
    
    async def route_alert(self, alert: Alert) -> List[AlertChannel]:
        """Determine which channels should receive this alert."""
        channels = []
        
        # Default routing based on severity
        if alert.labels.severity == AlertSeverity.CRITICAL:
            channels.append(AlertChannel.INFOFLOW)
            channels.append(AlertChannel.WEBHOOK)
        elif alert.labels.severity == AlertSeverity.WARNING:
            channels.append(AlertChannel.INFOFLOW)
        else:
            channels.append(AlertChannel.WEBHOOK)
        
        # Apply custom routing rules
        for match, channel in self._routing_rules:
            if self._matches(alert, match):
                if channel not in channels:
                    channels.append(channel)
        
        return channels
    
    def _matches(self, alert: Alert, match: Dict[str, Any]) -> bool:
        """Check if alert matches the routing rule."""
        for key, value in match.items():
            alert_value = getattr(alert.labels, key, None)
            if alert_value is None:
                alert_value = alert.labels.extra.get(key)
            
            if alert_value != value:
                return False
        return True


class AlertFormatter:
    """Format alerts for different notification channels."""
    
    @staticmethod
    def format_for_infoflow(alert: Alert) -> Dict[str, Any]:
        """Format alert for Infoflow (如流) notification."""
        severity_emoji = {
            AlertSeverity.CRITICAL: "🔴",
            AlertSeverity.WARNING: "🟡", 
            AlertSeverity.INFO: "🔵"
        }
        
        status_text = "触发" if alert.status == AlertStatus.FIRING else "恢复"
        emoji = severity_emoji.get(alert.labels.severity, "⚪")
        
        # Build mention list based on service
        mentions = []
        if alert.labels.severity == AlertSeverity.CRITICAL:
            # Critical alerts should mention service owners
            if alert.labels.service == "task-api":
                mentions.extend(["lishihu", "liuqiwen"])  # Example mentions
            elif alert.labels.service == "ops-api":
                mentions.extend(["zhbxlm", "lishihu"])
        
        message = f"{emoji} **告警{status_text}**: {alert.labels.alertname}\n\n"
        message += f"**服务**: {alert.labels.service or 'unknown'}\n"
        message += f"**严重程度**: {alert.labels.severity.value}\n"
        message += f"**时间**: {alert.starts_at.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        message += f"**摘要**: {alert.annotations.summary}\n"
        message += f"**详情**: {alert.annotations.description}\n"
        
        if alert.annotations.runbook_url:
            message += f"\n**处理手册**: {alert.annotations.runbook_url}"
        
        if alert.status == AlertStatus.RESOLVED and alert.ends_at:
            message += f"\n\n✅ 告警已于 {alert.ends_at.strftime('%H:%M:%S')} 恢复"
        
        return {
            "message": message,
            "mentions": mentions,
            "severity": alert.labels.severity
        }
    
    @staticmethod
    def format_for_email(alert: Alert) -> Dict[str, Any]:
        """Format alert for email notification."""
        subject = f"[{alert.status.value.upper()}] {alert.labels.severity.value.upper()}: {alert.labels.alertname}"
        
        body = f"""
Alert: {alert.labels.alertname}
Status: {alert.status.value}
Severity: {alert.labels.severity.value}
Service: {alert.labels.service or 'unknown'}
Time: {alert.starts_at.strftime('%Y-%m-%d %H:%M:%S UTC')}

Summary: {alert.annotations.summary}
Description: {alert.annotations.description}

Labels:
{json.dumps(alert.labels.model_dump(), indent=2, default=str)}

Generator URL: {alert.generator_url or 'N/A'}
Runbook URL: {alert.annotations.runbook_url or 'N/A'}
"""
        
        return {"subject": subject, "body": body}
    
    @staticmethod
    def format_for_webhook(alert: Alert) -> Dict[str, Any]:
        """Format alert for generic webhook (JSON)."""
        return alert.model_dump()


class AlertNotifier:
    """Send alerts to configured channels."""
    
    def __init__(self, router: AlertRouter):
        self.router = router
        self.formatter = AlertFormatter()
        
    async def notify(self, alert: Alert) -> Dict[AlertChannel, bool]:
        """Send notification to all appropriate channels."""
        channels = await self.router.route_alert(alert)
        results = {}
        
        for channel in channels:
            try:
                success = await self._send_to_channel(channel, alert)
                results[channel] = success
                logger.info("Alert %s sent to %s: %s", 
                           alert.labels.alertname, channel, success)
            except Exception as e:
                logger.error("Failed to send alert to %s: %s", channel, e)
                results[channel] = False
        
        return results
    
    async def _send_to_channel(self, channel: AlertChannel, alert: Alert) -> bool:
        """Send alert to specific channel."""
        # TODO: Implement actual sending logic
        # This is a placeholder - actual implementations would use:
        # - infoflow_send for Infoflow
        # - smtplib for email
        # - httpx for webhook
        
        logger.info("Would send alert to %s: %s", channel, alert.labels.alertname)
        
        # Simulate async sending
        await asyncio.sleep(0.01)
        
        # For now, just log
        if channel == AlertChannel.INFOFLOW:
            formatted = self.formatter.format_for_infoflow(alert)
            logger.info("Infoflow alert: %s", formatted["message"])
        elif channel == AlertChannel.EMAIL:
            formatted = self.formatter.format_for_email(alert)
            logger.info("Email alert subject: %s", formatted["subject"])
        
        return True


# Global alert router and notifier
_alert_router: Optional[AlertRouter] = None
_alert_notifier: Optional[AlertNotifier] = None


def get_alert_router() -> AlertRouter:
    """Get or create global alert router."""
    global _alert_router
    if _alert_router is None:
        _alert_router = AlertRouter()
        
        # Configure default routes
        _alert_router.add_route(
            {"service": "task-api", "severity": AlertSeverity.CRITICAL},
            AlertChannel.INFOFLOW
        )
        _alert_router.add_route(
            {"service": "ops-api", "severity": AlertSeverity.CRITICAL},
            AlertChannel.INFOFLOW
        )
        
    return _alert_router


def get_alert_notifier() -> AlertNotifier:
    """Get or create global alert notifier."""
    global _alert_notifier
    if _alert_notifier is None:
        router = get_alert_router()
        _alert_notifier = AlertNotifier(router)
    return _alert_notifier


async def process_alertmanager_webhook(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Process Alertmanager webhook payload."""
    try:
        webhook = AlertManagerWebhook(**payload)
        notifier = get_alert_notifier()
        
        results = {}
        for alert in webhook.alerts:
            alert_results = await notifier.notify(alert)
            results[alert.fingerprint] = alert_results
        
        return {
            "status": "success",
            "alerts_processed": len(webhook.alerts),
            "results": results
        }
    except Exception as e:
        logger.error("Failed to process alertmanager webhook: %s", e)
        return {
            "status": "error",
            "error": str(e)
        }


# Example usage
if __name__ == "__main__":
    # Test alert
    test_alert = Alert(
        status=AlertStatus.FIRING,
        labels=AlertLabel(
            alertname="ServiceDown",
            severity=AlertSeverity.CRITICAL,
            service="task-api",
            instance="task-api-1"
        ),
        annotations=AlertAnnotation(
            summary="task-api is down",
            description="Service task-api has been down for more than 1 minute",
            runbook_url="https://example.com/runbook"
        ),
        starts_at=datetime.now(timezone.utc),
        fingerprint="test-123"
    )
    
    # Create router and test
    router = AlertRouter()
    notifier = AlertNotifier(router)
    
    # Test formatting
    infoflow_msg = AlertFormatter.format_for_infoflow(test_alert)
    print("Infoflow message:")
    print(infoflow_msg["message"])
    print("\nMentions:", infoflow_msg["mentions"])