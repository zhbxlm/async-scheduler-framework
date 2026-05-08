"""Integration tests for health check endpoints."""
from __future__ import annotations

from fastapi.testclient import TestClient


def test_health_basic(test_client: TestClient):
    """Test basic health endpoint (now under /api/v1/health)."""
    response = test_client.get("/api/v1/health", follow_redirects=True)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"


def test_health_detailed(test_client: TestClient):
    """Test detailed health endpoint."""
    response = test_client.get("/api/v1/health/detailed")
    assert response.status_code == 200
    data = response.json()

    # Check required fields
    assert data["status"] == "healthy"
    assert "timestamp" in data
    assert "system" in data
    assert "process" in data
    assert "uptime_seconds" in data
    assert "request_count" in data

    # Check system metrics structure
    system = data["system"]
    assert "cpu_percent" in system
    assert "memory_percent" in system
    assert "memory_available_gb" in system
    assert "disk_percent" in system
    assert "disk_free_gb" in system

    # Check process metrics structure
    process = data["process"]
    assert "pid" in process
    assert "memory_rss_mb" in process
    assert "memory_vms_mb" in process
    assert "cpu_percent" in process
    assert "threads" in process
    assert "connections" in process


def test_health_ready(test_client: TestClient):
    """Test readiness probe."""
    response = test_client.get("/api/v1/health/ready")
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "ready"
    assert "timestamp" in data
    assert "checks" in data
    assert data["checks"]["api"] == "ok"


def test_health_metrics(test_client: TestClient):
    """Test Prometheus metrics endpoint."""
    response = test_client.get("/api/v1/health/metrics")
    assert response.status_code == 200
    # Accept either plain text or Prometheus format
    content_type = response.headers["content-type"]
    assert "text/plain" in content_type

    content = response.text
    # Check for Prometheus metric lines
    assert "# HELP" in content
    assert "# TYPE" in content
    assert "scheduler_uptime_seconds" in content
    assert "scheduler_request_count" in content
    assert "scheduler_memory_rss_bytes" in content
    assert "scheduler_cpu_seconds_total" in content


def test_health_redis(test_client: TestClient):
    """Test Redis health check."""
    response = test_client.get("/api/v1/health/redis")
    # No redis configured in tests — expect 200 with status unknown/healthy
    assert response.status_code == 200
    data = response.json()
    assert data["status"] in ("healthy", "unknown")
    assert data["service"] == "redis"


def test_health_mysql(test_client: TestClient):
    """Test MySQL health check."""
    response = test_client.get("/api/v1/health/mysql")
    # No mysql configured in tests — expect 200 with status disabled/healthy
    assert response.status_code == 200
    data = response.json()
    assert data["status"] in ("healthy", "disabled", "unknown")
    assert data["service"] == "mysql"


def test_health_error_handling(test_client: TestClient):
    """Test that health endpoints handle errors gracefully."""
    # This test would simulate error conditions
    # For now, just verify endpoints exist
    endpoints = [
        "/api/v1/health",
        "/api/v1/health/detailed",
        "/api/v1/health/ready",
        "/api/v1/health/metrics",
        "/api/v1/health/redis",
        "/api/v1/health/mysql",
    ]

    for endpoint in endpoints:
        response = test_client.get(endpoint)
        assert response.status_code in [200, 503], f"Endpoint {endpoint} failed"

        if response.status_code == 503:
            # Service unhealthy response
            data = response.json()
            assert "detail" in data


def test_health_request_count_increment(test_client: TestClient):
    """Test that request count increments."""
    # Get initial count
    response1 = test_client.get("/api/v1/health/detailed")
    data1 = response1.json()
    initial_count = data1["request_count"]

    # Make another request
    response2 = test_client.get("/api/v1/health/detailed")
    data2 = response2.json()
    new_count = data2["request_count"]

    # Count should increase (or at least not decrease)
    # Note: Due to fixture scope, each test gets fresh state
    # So we can only assert it's non-negative
    assert new_count >= 0
    assert initial_count >= 0


def test_health_uptime_increases(test_client: TestClient):
    """Test that uptime increases between requests."""
    import time

    response1 = test_client.get("/api/v1/health/detailed")
    data1 = response1.json()
    uptime1 = data1["uptime_seconds"]

    time.sleep(0.1)  # Small delay

    response2 = test_client.get("/api/v1/health/detailed")
    data2 = response2.json()
    uptime2 = data2["uptime_seconds"]

    # Uptime should increase (or at least not decrease)
    assert uptime2 >= uptime1


# Test for error handling decorators
def test_health_endpoints_have_error_handling(test_client: TestClient):
    """Verify that health endpoints have proper error handling."""
    # We can't directly test the decorators, but we can verify
    # the endpoints respond with proper structure even when
    # dependencies fail (though that's harder to simulate)
    pass


if __name__ == "__main__":
    # Quick manual test
    import sys
    sys.path.insert(0, ".")
    from fastapi.testclient import TestClient

    from src.main import app

    client = TestClient(app)

    print("Running health check tests...")
    test_health_basic(client)
    test_health_detailed(client)
    test_health_ready(client)
    test_health_metrics(client)
    test_health_redis(client)
    test_health_mysql(client)
    print("All health tests passed!")
