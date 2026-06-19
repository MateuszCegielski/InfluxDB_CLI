import typer
from typing import Annotated
from influxdb_cli.core.influx_client import InfluxClient

app = typer.Typer(name="modify", no_args_is_help=True)


@app.command(name="retention-policy", help="Modify a retention policy.")
def modify_retention_policy(
    retention_policy_name: Annotated[
        str, typer.Argument(help="Name of the retention policy to modify")
    ],
    new_duration: Annotated[
        str | None, typer.Option("--duration", "-n", help="New duration for the retention policy")
    ] = None,
    new_replication: Annotated[
        int | None,
        typer.Option("--replication", "-r", help="New replication factor for the retention policy"),
    ] = None,
    set_default: Annotated[
        bool, typer.Option("--default", "-s", help="Set retention policy to default.")
    ] = False,
    database_name: Annotated[
        str,
        typer.Option(
            "--database",
            "-d",
            help="Name of the database, if not specified " "the current database will be used.",
        ),
    ] = None,
):
    influx_client = InfluxClient()
    logs = influx_client.execute_method_on_db(
        method=influx_client.modify_retention_policy,
        database=database_name,
        retention_policy_name=retention_policy_name,
        new_duration=new_duration,
        new_replication=new_replication,
        set_default=set_default,
    )
    typer.echo(logs)
