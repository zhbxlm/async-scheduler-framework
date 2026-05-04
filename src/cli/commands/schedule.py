"""schedule commands."""
import click


@click.group()
def schedule():
    """Manage schedules."""
    pass


@schedule.command()
def list():
    """List schedules."""
    click.echo(f"Listing schedules (not implemented)")


@schedule.command()
@click.argument("id")
def get(id):
    """Get schedule details."""
    click.echo(f"Getting schedule {id} (not implemented)")
