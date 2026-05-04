"""capability commands."""
import click


@click.group()
def capability():
    """Manage capabilitys."""
    pass


@capability.command()
def list():
    """List capabilitys."""
    click.echo(f"Listing capabilitys (not implemented)")


@capability.command()
@click.argument("id")
def get(id):
    """Get capability details."""
    click.echo(f"Getting capability {id} (not implemented)")
