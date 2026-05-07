"""scheduler_task_api.server — CLI entry point."""
from __future__ import annotations

import click
import uvicorn

from scheduler_task_api.app import create_app


@click.command()
@click.option("--host", default="0.0.0.0", envvar="TASK_API_HOST")
@click.option("--port", default=8001, type=int, envvar="TASK_API_PORT")
@click.option("--workers", default=1, type=int, envvar="TASK_API_WORKERS")
@click.option("--log-level", default="info", envvar="LOG_LEVEL")
@click.option("--reload", is_flag=True, default=False, help="Enable hot reload (dev only)")
def main(host: str, port: int, workers: int, log_level: str, reload: bool) -> None:
    """Start the async-scheduler Task API server."""
    app = create_app()
    uvicorn.run(
        app,
        host=host,
        port=port,
        workers=workers if not reload else 1,
        log_level=log_level,
        reload=reload,
    )


if __name__ == "__main__":
    main()
