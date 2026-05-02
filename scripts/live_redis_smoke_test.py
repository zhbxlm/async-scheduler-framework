from __future__ import annotations

import os
import subprocess
import sys


def main() -> int:
    redis_url = os.getenv("TEST_REDIS_URL")
    if not redis_url:
        print("TEST_REDIS_URL is not set; skipping live Redis smoke test.")
        return 0

    cmd = [sys.executable, "-m", "pytest", "-q", "tests/integration/test_live_redis_smoke.py"]
    print(f"Running live Redis smoke against {redis_url} ...")
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
