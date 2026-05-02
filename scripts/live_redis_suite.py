from __future__ import annotations

import os
import subprocess
import sys
from typing import Sequence


TEST_GROUPS: dict[str, list[str]] = {
    "smoke": [
        "tests/integration/test_live_redis_smoke.py",
    ],
    "recovery": [
        "tests/integration/test_live_redis_recovery.py",
        "tests/integration/test_live_redis_consumer_recovery.py",
    ],
    "overlap": [
        "tests/integration/test_live_redis_completion_overlap.py",
        "tests/integration/test_live_redis_duplicate_completion_overlap.py",
        "tests/integration/test_live_redis_lease_loss_completion_race.py",
        "tests/integration/test_live_redis_attempt_consistency_overlap.py",
        "tests/integration/test_live_redis_multi_worker_overlap.py",
        "tests/integration/test_live_redis_multi_worker_delayed_promotion.py",
        "tests/integration/test_live_redis_multi_worker_dead_owner_recovery.py",
        "tests/integration/test_live_redis_end_to_end_consumer_loop.py",
        "tests/integration/test_live_redis_external_job_crash_recovery.py",
    ],
}
TEST_GROUPS["all"] = TEST_GROUPS["smoke"] + TEST_GROUPS["recovery"] + TEST_GROUPS["overlap"]


def run(cmd: Sequence[str]) -> int:
    print("+", " ".join(cmd))
    return subprocess.call(list(cmd))


def main() -> int:
    redis_url = os.getenv("TEST_REDIS_URL")
    if not redis_url:
        print("TEST_REDIS_URL is not set; skipping live Redis suite.")
        return 0

    args = sys.argv[1:]
    group = "all"
    fail_fast = False
    for arg in args:
        if arg == "--fail-fast":
            fail_fast = True
        elif arg.startswith("--"):
            print(f"Unknown option: {arg}")
            return 2
        else:
            group = arg

    if group not in TEST_GROUPS:
        valid = ", ".join(sorted(TEST_GROUPS))
        print(f"Unknown group: {group}. Valid groups: {valid}")
        return 2

    selected = TEST_GROUPS[group]
    mode = "fail-fast" if fail_fast else "continue-on-error"
    print(f"Running live Redis suite group '{group}' against {redis_url} ({mode})")
    failures = 0
    for test_path in selected:
        code = run([sys.executable, "-m", "pytest", "-q", test_path])
        if code != 0:
            failures += 1
            if fail_fast:
                print(f"Stopping early after failure in {test_path}")
                return 1

    if failures:
        print(f"Live Redis suite group '{group}' completed with {failures} failing segment(s).")
        return 1

    print(f"Live Redis suite group '{group}' passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
