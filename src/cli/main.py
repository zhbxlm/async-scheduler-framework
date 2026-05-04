"""CLI entry point — kubectl-style commands."""
import os
import click
from src.cli.client import ApiClient


@click.group()
@click.option("--api-url", envvar="RAY_ASYNC_API_URL", default="http://localhost:8000", help="API server URL")
@click.pass_context
def cli(ctx, api_url):
    ctx.ensure_object(dict)
    ctx.obj["client"] = ApiClient(base_url=api_url)


# Import all command groups
from src.cli.commands.capability import capability
from src.cli.commands.cluster import cluster
from src.cli.commands.dag import dag
from src.cli.commands.deploy import deploy
from src.cli.commands.node import node
from src.cli.commands.queue import queue
from src.cli.commands.schedule import schedule
from src.cli.commands.task import task
from src.cli.commands.tenant import tenant
from src.cli.commands.worker import worker

cli.add_command(capability)
cli.add_command(cluster)
cli.add_command(dag)
cli.add_command(deploy)
cli.add_command(node)
cli.add_command(queue)
cli.add_command(schedule)
cli.add_command(task)
cli.add_command(tenant)
cli.add_command(worker)

if __name__ == "__main__":
    cli()
