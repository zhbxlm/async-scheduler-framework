"""Alert management endpoints for Prometheus Alertmanager integration."""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from src.monitoring.alerts import (
    Alert,
    AlertManagerWebhook,
    AlertSeverity,
    AlertStatus,
    process_alertmanager_webhook,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.post("/webhook")
async def alertmanager_webhook(request: Request) -> Dict[str, Any]:
    """Receive alerts from Prometheus Alertmanager.
    
    This endpoint conforms to the Alertmanager webhook format:
    https://prometheus.io/docs/alerting/latest/configuration/#webhook_config
    
    Example payload:
    {
        "version": "4",
        "groupKey": "{}:{alertname=\"ServiceDown\"}",
        "truncatedAlerts": 0,
        "status": "firing",
        "receiver": "webhook",
        "groupLabels": {},
        "commonLabels": {
            "alertname": "ServiceDown",
            "severity": "critical"
        },
        "commonAnnotations": {
            "summary": "task-api is down"
        },
        "externalURL": "http://alertmanager:9093",
        "alerts": [...]
    }
    """
    try:
        payload = await request.json()
        logger.info("Received alertmanager webhook: %s alerts", 
                   len(payload.get("alerts", [])))
        
        result = await process_alertmanager_webhook(payload)
        
        if result["status"] == "error":
            logger.error("Failed to process alerts: %s", result.get("error"))
            raise HTTPException(status_code=400, detail=result["error"])
        
        return result
        
    except Exception as e:
        logger.error("Error processing webhook: %s", e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/status")
async def get_alert_status() -> Dict[str, Any]:
    """Get alert system status and configuration."""
    # TODO: Implement alert status monitoring
    return {
        "status": "operational",
        "channels": ["infoflow", "webhook"],
        "last_alert": None,
        "uptime": "0s"  # Would be calculated
    }


@router.get("/test")
async def test_alert() -> Dict[str, Any]:
    """Send a test alert to verify the alert system is working.
    
    This endpoint simulates an Alertmanager webhook for testing.
    """
    import datetime
    
    test_payload = {
        "version": "4",
        "groupKey": "test:{}",
        "status": "firing",
        "receiver": "webhook",
        "groupLabels": {},
        "commonLabels": {
            "alertname": "TestAlert",
            "severity": "warning",
            "service": "task-api"
        },
        "commonAnnotations": {
            "summary": "Test alert from alert system",
            "description": "This is a test alert to verify the alert system is working correctly."
        },
        "externalURL": "http://localhost:9093",
        "alerts": [
            {
                "status": "firing",
                "labels": {
                    "alertname": "TestAlert",
                    "severity": "warning",
                    "service": "task-api",
                    "instance": "test-instance"
                },
                "annotations": {
                    "summary": "Test alert from alert system",
                    "description": "This is a test alert to verify the alert system is working correctly.",
                    "runbook_url": "https://github.com/zhbxlm/async-scheduler-framework/docs/TESTING.md"
                },
                "startsAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "endsAt": None,
                "generatorURL": "http://localhost:9090",
                "fingerprint": "test-fingerprint-123"
            }
        ]
    }
    
    try:
        result = await process_alertmanager_webhook(test_payload)
        return {
            "status": "success",
            "message": "Test alert sent successfully",
            "result": result
        }
    except Exception as e:
        logger.error("Test alert failed: %s", e)
        return {
            "status": "error",
            "error": str(e)
        }


# Mock endpoints for alert management (to be implemented)
@router.get("/")
async def list_alerts(
    status: AlertStatus | None = None,
    severity: AlertSeverity | None = None,
    service: str | None = None,
    limit: int = 100,
) -> Dict[str, Any]:
    """List recent alerts (mock implementation)."""
    # TODO: Implement alert history storage and retrieval
    return {
        "alerts": [],
        "total": 0,
        "filters": {
            "status": status,
            "severity": severity,
            "service": service
        }
    }


@router.post("/silence")
async def create_silence(
    alertname: str | None = None,
    severity: AlertSeverity | None = None,
    service: str | None = None,
    duration_minutes: int = 60,
) -> Dict[str, Any]:
    """Create a temporary silence for matching alerts (mock)."""
    # TODO: Implement alert silence management
    return {
        "status": "success",
        "message": f"Silence created for {duration_minutes} minutes",
        "silence_id": "mock-silence-id",
        "filters": {
            "alertname": alertname,
            "severity": severity,
            "service": service
        }
    }


@router.delete("/silence/{silence_id}")
async def delete_silence(silence_id: str) -> Dict[str, Any]:
    """Delete a silence (mock)."""
    # TODO: Implement silence deletion
    return {
        "status": "success",
        "message": f"Silence {silence_id} deleted"
    }


@router.post("/acknowledge/{fingerprint}")
async def acknowledge_alert(fingerprint: str) -> Dict[str, Any]:
    """Acknowledge an alert (mark as being handled)."""
    # TODO: Implement alert acknowledgment tracking
    return {
        "status": "success",
        "message": f"Alert {fingerprint} acknowledged"
    }