"""queue commands."""
import click


@click.group()
def queue():
    """Manage queues."""
    pass


@queue.command()
def list():
    """List queues."""
    click.echo(f"Listing queues (not implemented)")


@queue.command()
@click.argument("id")
def get(id):
    """Get queue details."""
    click.echo(f"Getting queue {id} (not implemented)")
