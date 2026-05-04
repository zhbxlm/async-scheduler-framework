"""dag commands."""
import click


@click.group()
def dag():
    """Manage dags."""
    pass


@dag.command()
def list():
    """List dags."""
    click.echo(f"Listing dags (not implemented)")


@dag.command()
@click.argument("id")
def get(id):
    """Get dag details."""
    click.echo(f"Getting dag {id} (not implemented)")
