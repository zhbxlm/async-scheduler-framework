"""RayManager — idempotent ray start/stop lifecycle."""
from __future__ import annotations
import orjson
import logging
import subprocess

logger = logging.getLogger(__name__)
_RAY_TIMEOUT = 30


def _is_ray_running() -> bool:
    try:
        result = subprocess.run(["ray", "status"], capture_output=True, timeout=10)
        return result.returncode == 0
    except Exception:
        return False


def ray_start(head_address: str, custom_resources: dict | None = None) -> bool:
    """Start Ray worker. Idempotent — returns True if already running."""
    if _is_ray_running():
        logger.info("RayManager: Ray already running, skipping start")
        return True

    cmd = ["ray", "start", f"--address={head_address}"]
    if custom_resources:
        cmd.append(f"--resources={orjson.dumps(custom_resources)}")

    try:
        result = subprocess.run(cmd, capture_output=True, timeout=_RAY_TIMEOUT, text=True)
        if result.returncode != 0:
            logger.error("RayManager: ray start failed: %s", result.stderr)
            return False
    except subprocess.TimeoutExpired:
        logger.error("RayManager: ray start timed out")
        return False

    # Verify
    if _is_ray_running():
        logger.info("RayManager: Ray started and verified")
        return True
    logger.error("RayManager: Ray start returned 0 but status check failed")
    return False


def ray_stop() -> bool:
    """Stop Ray worker. Idempotent — returns True if already stopped."""
    if not _is_ray_running():
        logger.info("RayManager: Ray not running, skip stop")
        return True
    try:
        result = subprocess.run(["ray", "stop"], capture_output=True, timeout=_RAY_TIMEOUT, text=True)
        if result.returncode != 0:
            logger.error("RayManager: ray stop failed: %s", result.stderr)
            return False
        return True
    except subprocess.TimeoutExpired:
        logger.error("RayManager: ray stop timed out")
        return False
