import typer

from influxdb_cli.core.app_runner import AppRunner, clean_up
from influxdb_cli.core.influx_client import InfluxClient

app = typer.Typer(name="measurement")

@app.command(name="run", help="Run an application measurement.")
def run_app(
        container_name: str = typer.Argument(help="Name of the application container "
                                                  "to run measurement on"),
        config_path: str = typer.Argument(help="Path to the application config file"),
        check_interval: int = typer.Option(5, "--check-interval", "-c",
                                          help="Interval in seconds to check container status"),
        database_prefix: str = typer.Option("test", "--database-prefix", "-d",
                                          help="Prefix for the databases to be used.")
):
    """Run measurement for a specified application container."""
    app_runner = AppRunner(
        app_config_path=config_path,
        docker_container_name=container_name,
        influxdb_cli=InfluxClient()
    )
    app_runner.run(check_interval_sec=check_interval, database_prefix=database_prefix)

@app.command(name="clean-up", help="Clean up measurement results for an application.")
def clean_up(
        database_prefix: str = typer.Argument(help="Prefix of the databases to clean up"),
):
    """Clean up measurement results for a specified application container."""
    clean_up(database_prefix)
    typer.echo(f"Cleaned up measurements for databases which starts with '{database_prefix}'")