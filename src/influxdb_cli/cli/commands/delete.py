import typer
from typing import Annotated
from influxdb_cli.core.influx_client import InfluxClient

app = typer.Typer(
    name="delete", no_args_is_help=True, help="Delete a single object from the InfluxDB."
)


@app.command(name="measurement", help="Delete a measurement from a database.")
def delete_measurement(
    measurement_name: Annotated[str, typer.Argument(help="Name of the measurement to delete")],
    database_name: Annotated[
        str | None, typer.Option("--database", "-d", help="Name of the database to delete")
    ] = None,
):
    """Delete a measurement from the specified database."""
    client = InfluxClient()
    client.execute_method_on_db(
        method=client.delete_measurement,
        database=database_name,
    )
    typer.echo(
        f"Measurement '{measurement_name}' deleted from database: " f"{client.config.database}."
    )


@app.command(name="measurements", help="Delete ALL measurements from the specified database.")
def delete_all_measurements(
    database_name: Annotated[
        str | None,
        typer.Option(
            "--database-name",
            "-d",
            help="Name of the database to clean, if not specified "
            "the current database will be used.",
        ),
    ] = None,
    exclude_measurements: Annotated[
        list[str] | None,
        typer.Option(
            "--except",
            "-e",
            help="Measurement(s) to exclude. " "Repeat option: --except m1 --except m2",
        ),
    ] = None,
):
    influx_client = InfluxClient()
    logs = influx_client.execute_method_on_db(
        method=influx_client.clean_database,
        database=database_name,
        exclude_measurements=exclude_measurements,
    )
    typer.echo(logs)


@app.command(name="database", help="Delete a single database.", no_args_is_help=True)
def delete_database(
    database_name: Annotated[str, typer.Argument(help="Name of the database to delete")] = "",
    delete_all_databases: Annotated[
        bool, typer.Option("--all", help="Delete all databases (use with caution!)")
    ] = False,
):
    def drop_database(influx_client: InfluxClient, database: str):
        try:
            influx_client.delete_database(database)
            typer.echo(f"Database '{database}' deleted successfully.")
        except ValueError as e:
            typer.echo(f"Database '{database}' does not exist.", err=True)
            raise typer.Exit()

    influx_client = InfluxClient()
    if delete_all_databases:
        confirm = typer.confirm(
            "Are you sure you want to delete ALL databases? This action cannot be undone."
        )
        if not confirm:
            typer.echo("Operation cancelled.")
            raise typer.Exit()
        databases = influx_client.list_databases()
        for db in databases:
            drop_database(influx_client, db)
    else:
        drop_database(influx_client, database_name)
