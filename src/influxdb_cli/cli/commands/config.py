import typer
from influxdb_cli.config.config_manager import load_config, get_user_config_path

app = typer.Typer()

@app.command("show")
def show_config():
    """Show current configuration settings."""
    typer.echo("Current configuration:")
    typer.echo(load_config().model_dump_json(indent=2))

@app.command("show-config-path")
def show_user_config_path():
    typer.echo("Path to user config:")
    typer.echo(get_user_config_path())
