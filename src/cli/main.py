"""CLI entry point — kubectl-style commands."""
import os
import click
from src.cli.client import ApiClient


@click.group()
@click.option("--api-url", envvar="SCHEDULER_API_URL", default="http://localhost:8000", help="API server URL")
@click.pass_context
def cli(ctx, api_url):
    ctx.ensure_object(dict)
    ctx.obj["client"] = ApiClient(base_url=api_url)


if __name__ == "__main__":
    cli()
