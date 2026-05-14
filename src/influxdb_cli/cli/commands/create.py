import typer
from typing import Annotated

from influxdb_cli.core.influx_client import InfluxClient

app = typer.Typer(name="create", no_args_is_help=True)


@app.command(name="database", help="Create a new database.")
def create_database(
    database_name: Annotated[str, typer.Argument(help="Name of the database to create")],
    retention_policy: Annotated[
        bool,
        typer.Option(
            "--retention-policy",
            "-r",
            help="Create retention policies as defined in the config file. "
            "Use command `show config` to display configuration",
        ),
    ] = False,
):
    influx_client = InfluxClient()
    influx_client.create_database(database_name, retention_policy)

    typer.echo(f"Database {database_name} has been created successfully.")
