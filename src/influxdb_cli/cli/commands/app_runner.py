import typer

from influxdb_cli.core.app_runner import AppRunner
from influxdb_cli.core.influx_client import InfluxClient

app = typer.Typer(name="measurement")

@app.command(name="run", help="Run an application measurement.")
def run_app(
        container_name: str = typer.Argument(help="Name of the application container "
                                                  "to run measurement on"),
        config_path: str = typer.Argument(help="Path to the application config file"),
        check_interval: int = typer.Option(5, "--check-interval", "-c",
                                          help="Interval in seconds to check container status")
):
    """Run measurement for a specified application container."""
    app_runner = AppRunner(
        app_config_path=config_path,
        docker_container_name=container_name,
        influxdb_cli=InfluxClient()
    )
    app_runner.run(check_interval_sec=check_interval)

@app.command(name="clean-up", help="Clean up measurement results for an application.")
def clean_up(
        container_name: str = typer.Argument(help="Name of the application container "
                                                  "to clean up measurement results from"),
        config_path: str = typer.Argument(help="Path to the application config file")
):
    """Clean up measurement results for a specified application container."""
    app_runner = AppRunner(
        app_config_path=config_path,
        docker_container_name=container_name,
        influxdb_cli=InfluxClient()
    )
    app_runner.clean_up()
    typer.echo("Cleaned up measurement results for container {}".format(container_name))