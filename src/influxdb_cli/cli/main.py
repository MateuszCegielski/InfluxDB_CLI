import typer
from influxdb_cli.cli.commands import app_runner, add, show, delete, create, modify

from influxdb_cli.core.influx_client import InfluxClient

app = typer.Typer(name="Main Commands", help="Main Commands:", add_completion=False, no_args_is_help=True)

app.add_typer(app_runner.app, name="app-runner", help="Run application tests.")
app.add_typer(create.app, name="create", help="Command used to create. ")
app.add_typer(add.app, name="add", help="Command used to add. ")
app.add_typer(show.app, name="show", help="Command used to show. ")
app.add_typer(modify.app, name="modify", help="Command used to modify. ")
app.add_typer(delete.app, name="delete", help="Command used to delete. ")

@app.command(name="query", help="Execute a custom InfluxDB query.")
def query():
    influx_client = InfluxClient()
    query_str = typer.prompt("Enter your InfluxDB query")
    result = influx_client.query(query_str)
    typer.echo("Query Result:")
    for point in result.get_points():
        typer.echo(point)

@app.command(name="use", help="Set the active database for the session.")
def use_database(
        database_name: str = typer.Argument(help="Name of the database to use")
):
    influx_client = InfluxClient()
    influx_client.ensure_database_exists(database_name)
    influx_client.switch_database(database_name)
    typer.echo(f"Active database set to '{database_name}'.")


if __name__ == "__main__":
    try:
        app()
    except ValueError as e:
        typer.secho(f"\nValue Error: {e}", err=True, fg=typer.colors.RED)
    except KeyboardInterrupt:
        typer.echo("\nExiting...")
        exit(0)
    except ConnectionError as e:
        typer.echo(f"\nConnection Error: {e}", err=True)
        exit(1)