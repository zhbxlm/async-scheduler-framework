"""node commands."""
import click


@click.group()
def node():
    """Manage nodes."""
    pass


@node.command()
def list():
    """List nodes."""
    click.echo(f"Listing nodes (not implemented)")


@node.command()
@click.argument("id")
def get(id):
    """Get node details."""
    click.echo(f"Getting node {id} (not implemented)")
