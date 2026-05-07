"""CLI entry point — kubectl-style commands.

The CLI connects to task-api (port 8001) by default since most operations
(task creation, status, cancellation) require the task API.  Ops commands
(capability, node, cluster, schedule management) are routed to the same
URL for simplicity — in a split deployment, point --api-url at the
appropriate service."""
import click
from scheduler_cli.client import ApiClient


@click.group()
@click.option(
    "--api-url",
    envvar="RAY_ASYNC_API_URL",
    default="http://localhost:8001",
    help="Task API base URL (port 8001). Override for ops commands if needed."
)
@click.pass_context
def cli(ctx, api_url):
    ctx.ensure_object(dict)
    ctx.obj["client"] = ApiClient(base_url=api_url)


# Import all command groups
from scheduler_cli.commands.capability import capability
from scheduler_cli.commands.cluster import cluster
from scheduler_cli.commands.dag import dag
from scheduler_cli.commands.deploy import deploy
from scheduler_cli.commands.node import node
from scheduler_cli.commands.queue import queue
from scheduler_cli.commands.schedule import schedule
from scheduler_cli.commands.task import task
from scheduler_cli.commands.tenant import tenant
from scheduler_cli.commands.worker import worker

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
