"""Alert management endpoints for Prometheus Alertmanager integration."""
from __future__ import annotations

import json
import logging
import time
import uuid

from src.common.ttl_constants import ALERT_ACK_TTL
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from src.monitoring.alerts import (
    AlertSeverity,
    AlertStatus,
    process_alertmanager_webhook,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/alerts", tags=["alerts"])

# Redis key prefixes for alert state
_ALERT_HISTORY_KEY = "alerts:history"       # ZSET: score=timestamp, member=json
_ALERT_SILENCE_KEY = "alerts:silences"      # HASH: silence_id → json
_ALERT_ACK_KEY = "alerts:ack:{fingerprint}" # SET
_ALERT_HISTORY_TTL = 86400 * 7              # keep 7 days
_ALERT_HISTORY_MAXLEN = 1000                # max entries in history


def _get_redis(request: Request):
    """Get Redis client from app state."""
    return getattr(request.app.state, "redis_client", None)


@router.post("/webhook")
async def alertmanager_webhook(request: Request) -> Dict[str, Any]:
    """Receive alerts from Prometheus Alertmanager.

    This endpoint conforms to the Alertmanager webhook format:
    https://prometheus.io/docs/alerting/latest/configuration/#webhook_config
    """
    try:
        payload = await request.json()

        result = await process_alertmanager_webhook(payload)

        # Persist alerts to Redis history
        redis = _get_redis(request)
        if redis:
            for alert in payload.get("alerts", []):
                entry = json.dumps({
                    "fingerprint": alert.get("fingerprint", ""),
                    "alertname": alert.get("labels", {}).get("alertname", ""),
                    "severity": alert.get("labels", {}).get("severity", ""),
                    "service": alert.get("labels", {}).get("service", ""),
                    "status": alert.get("status", "firing"),
                    "summary": alert.get("annotations", {}).get("summary", ""),
                    "received_at": time.time(),
                })
                try:
                    ts = time.time()
                    await redis.zadd(_ALERT_HISTORY_KEY, {entry: ts})
                    # Trim to max length
                    await redis.zremrangebyrank(_ALERT_HISTORY_KEY, 0, -_ALERT_HISTORY_MAXLEN - 1)
                except Exception as e:
                    logger.warning("Failed to persist alert to Redis: %s", e)

        if result["status"] == "error":
            logger.error("Failed to process alerts: %s", result.get("error"))
            raise HTTPException(status_code=400, detail=result["error"])

        return result

    except Exception as e:
        logger.error("Error processing webhook: %s", e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/status")
async def get_alert_status(request: Request) -> Dict[str, Any]:
    """Get alert system status and configuration."""
    redis = _get_redis(request)
    total_alerts = 0
    last_alert_ts = None

    if redis:
        try:
            total_alerts = await redis.zcard(_ALERT_HISTORY_KEY)
            # Get the most recent alert
            recent = await redis.zrevrange(_ALERT_HISTORY_KEY, 0, 0, withscores=True)
            if recent:
                last_alert_ts = recent[0][1]
        except Exception as e:
            logger.warning("Failed to get alert stats: %s", e)

    return {
        "status": "operational",
        "channels": ["infoflow", "webhook"],
        "total_alerts_stored": total_alerts,
        "last_alert_ts": last_alert_ts,
        "history_ttl_days": 7,
    }


@router.get("/test")
async def test_alert() -> Dict[str, Any]:
    """Send a test alert to verify the alert system is working."""
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
                    "description": "This is a test alert.",
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
        return {"status": "success", "message": "Test alert sent successfully", "result": result}
    except Exception as e:
        logger.error("Test alert failed: %s", e)
        return {"status": "error", "error": str(e)}


@router.get("/")
async def list_alerts(
    request: Request,
    status: AlertStatus | None = None,
    severity: AlertSeverity | None = None,
    service: str | None = None,
    limit: int = 100,
) -> Dict[str, Any]:
    """List recent alerts from Redis history."""
    redis = _get_redis(request)
    alerts = []

    if redis:
        try:
            # Fetch recent entries (newest first)
            raw_entries = await redis.zrevrange(_ALERT_HISTORY_KEY, 0, limit - 1)
            for raw in raw_entries:
                try:
                    entry = json.loads(raw)
                    # Apply filters
                    if status and entry.get("status") != status.value:
                        continue
                    if severity and entry.get("severity") != severity.value:
                        continue
                    if service and entry.get("service") != service:
                        continue
                    alerts.append(entry)
                except Exception:
                    pass
        except Exception as e:
            logger.warning("Failed to fetch alert history: %s", e)

    return {
        "alerts": alerts,
        "total": len(alerts),
        "filters": {"status": status, "severity": severity, "service": service},
    }


@router.post("/silence")
async def create_silence(
    request: Request,
    alertname: str | None = None,
    severity: AlertSeverity | None = None,
    service: str | None = None,
    duration_minutes: int = 60,
) -> Dict[str, Any]:
    """Create a temporary silence for matching alerts (stored in Redis)."""
    silence_id = str(uuid.uuid4())[:8]
    silence = {
        "silence_id": silence_id,
        "alertname": alertname,
        "severity": severity.value if severity else None,
        "service": service,
        "duration_minutes": duration_minutes,
        "created_at": time.time(),
        "expires_at": time.time() + duration_minutes * 60,
    }

    redis = _get_redis(request)
    if redis:
        try:
            await redis.hset(_ALERT_SILENCE_KEY, silence_id, json.dumps(silence))
            await redis.expire(_ALERT_SILENCE_KEY, duration_minutes * 60 + 3600)
        except Exception as e:
            logger.warning("Failed to save silence to Redis: %s", e)

    return {
        "status": "success",
        "message": f"Silence created for {duration_minutes} minutes",
        "silence_id": silence_id,
        "silence": silence,
    }


@router.delete("/silence/{silence_id}")
async def delete_silence(silence_id: str, request: Request) -> Dict[str, Any]:
    """Delete a silence."""
    redis = _get_redis(request)
    deleted = False

    if redis:
        try:
            deleted = bool(await redis.hdel(_ALERT_SILENCE_KEY, silence_id))
        except Exception as e:
            logger.warning("Failed to delete silence from Redis: %s", e)

    if not deleted:
        raise HTTPException(status_code=404, detail=f"Silence {silence_id} not found")

    return {"status": "success", "message": f"Silence {silence_id} deleted"}


@router.post("/acknowledge/{fingerprint}")
async def acknowledge_alert(fingerprint: str, request: Request) -> Dict[str, Any]:
    """Acknowledge an alert (mark as being handled), stored in Redis."""
    ack_key = _ALERT_ACK_KEY.format(fingerprint=fingerprint)
    ack_data = json.dumps({
        "fingerprint": fingerprint,
        "acknowledged_at": time.time(),
    })

    redis = _get_redis(request)
    if redis:
        try:
            await redis.set(ack_key, ack_data, ex=ALERT_ACK_TTL)
        except Exception as e:
            logger.warning("Failed to save acknowledgment to Redis: %s", e)

    return {
        "status": "success",
        "message": f"Alert {fingerprint} acknowledged",
        "acknowledged_at": time.time(),
    }
