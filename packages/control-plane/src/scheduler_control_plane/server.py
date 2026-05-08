"""scheduler_control_plane.server — CLI entry point."""
from __future__ import annotations

import asyncio

from scheduler_control_plane.app import create_runtime


def main() -> None:
    runtime = create_runtime()
    asyncio.run(runtime())


if __name__ == "__main__":
    main()
