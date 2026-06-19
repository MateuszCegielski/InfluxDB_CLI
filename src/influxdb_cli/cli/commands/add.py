import typer
from typing import Annotated
from pathlib import Path

from influxdb_cli.core.influx_client import InfluxClient

app = typer.Typer(
    name="add",
    no_args_is_help=True,
    help="Commands to add measurements to the database from files.",
)


@app.command(name="measurement", help="Add a single measurement to the database from file.")
def add_measurement(
    measurement_name: Annotated[str, typer.Argument(help="Name of the measurement")],
    path: Annotated[str | None, typer.Option("--path", "-p", help="Path to the file.")],
    database_name: Annotated[
        str | None,
        typer.Option(
            "--database_name",
            "-d",
            help="Name of the database. If not specified, the current will used.",
        ),
    ] = None,
    add_batch_timestamp: Annotated[
        bool,
        typer.Option(
            "--add_batch_timestamp",
            "-a",
            help="Add the first timestamp of data to batch_timestamp measurement.",
        ),
    ] = False,
):
    """Add a measurement to the database from file.
    Parameters
    ----------
    measurement_name : str
        Name of the measurement - required.
    path : str
        Path to the file.
    database_name : str, optional
        Name of the database. If not specified, the current will used.
    add_batch_timestamp : bool, optional
        Add the first timestamp of data to batch_timestamp measurement.
    """

    client = InfluxClient()
    if not Path(path).is_file():
        typer.echo("Error: Provided path is not a file.")
        return
    measurements = client.execute_method_on_db(
        method=client.add_measurements,
        database=database_name,
        file_path=path,
        measurement_name=measurement_name,
        add_batch_timestamp=add_batch_timestamp,
    )
    typer.echo(
        f"Added {measurements} measurements to database: "
        f"{database_name or client.config.database}."
    )


@app.command(
    name="measurements",
    help="Add measurements from all files in a directory."
    "A database is created per file, using the file name as the database name",
)
def add_measurements(
    measurement_name: Annotated[str, typer.Argument(help="Name of the measurement")],
    path: Annotated[str | None, typer.Option("--path", "-p", help="Path to the directory.")],
    add_batch_timestamp: Annotated[
        bool,
        typer.Option(
            "--add_batch_timestamp",
            "-a",
            help="Add the first timestamp of data to batch_timestamp measurement.",
        ),
    ] = False,
):
    """Import data files from a directory.
    For each file, the CLI creates (or uses) a database named after the file (without extension),
    and writes the file's data as a measurement.
    Parameters
    ----------
    measurement_name : str
        Name for the measurements - required.
    path : str
        Path to the directory.
    add_batch_timestamp : bool, optional
        Add the first timestamp of data to batch_timestamp measurement.
    """
    client = InfluxClient()
    if not Path(path).is_dir():
        typer.echo("Error: Provided path is not a directory.")
        return

    client.add_measurement_from_dir(
        file_path=path,
        measurement_name=measurement_name,
        add_batch_timestamp=add_batch_timestamp,
    )
    typer.echo(f"Created databases and added measurements from directory: {path}.")


@app.command(
    name="batch-timestamp", help="Add the first timestamp of data to batch_timestamp measurement."
)
def add_batch_timestamp(
    measurement_name: Annotated[str, typer.Argument(help="Name of the measurement")],
    batch_timestamp_measurement_name: Annotated[
        str | None,
        typer.Option(
            "--batch-timestamp-measurement-name",
            "-b",
            help="Name of the measurement to store batch timestamp. Default is 'batch_timestamps'.",
        ),
    ] = None,
    database_name: Annotated[
        str | None,
        typer.Option(
            "--database_name",
            "-d",
            help="Name of the database. If not defined the current one will be used.",
        ),
    ] = None,
):
    """Add the first timestamp of data to batch_timestamp measurement."""
    client = InfluxClient()
    logs = client.execute_method_on_db(
        method=client.add_first_timestamp_to_batch_measurement,
        database=database_name,
        measurement_name=measurement_name,
        batch_measurement_name=batch_timestamp_measurement_name,
    )
    typer.echo(logs)
