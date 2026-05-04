"""worker commands."""
import click


@click.group()
def worker():
    """Manage workers."""
    pass


@worker.command()
def list():
    """List workers."""
    click.echo(f"Listing workers (not implemented)")


@worker.command()
@click.argument("id")
def get(id):
    """Get worker details."""
    click.echo(f"Getting worker {id} (not implemented)")
