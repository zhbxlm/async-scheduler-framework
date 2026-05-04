"""task commands."""
import click


@click.group()
def task():
    """Manage tasks."""
    pass


@task.command()
def list():
    """List tasks."""
    click.echo(f"Listing tasks (not implemented)")


@task.command()
@click.argument("id")
def get(id):
    """Get task details."""
    click.echo(f"Getting task {id} (not implemented)")
