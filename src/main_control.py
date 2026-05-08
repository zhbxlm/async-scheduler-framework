"""Control-plane worker entry point."""
from __future__ import annotations

import asyncio

from src.runtime_control_plane import run_forever


if __name__ == "__main__":
    asyncio.run(run_forever())
