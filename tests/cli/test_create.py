from typer.testing import CliRunner
import influxdb_cli.cli.commands.create as create_module

runner = CliRunner()


class DummyClient:
    def __init__(self):
        self.calls = []

    def create_database(self, database_name, retention_policy):
        self.calls.append((database_name, retention_policy))


def test_create_help():
    result = runner.invoke(create_module.app, ["--help"])
    assert result.exit_code == 0
    assert "database" in result.stdout


def test_create_database_direct_call(monkeypatch, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(create_module, "InfluxClient", lambda: dummy)

    create_module.create_database("db_new", retention_policy=True)

    captured = capsys.readouterr()
    assert "Database db_new has been created successfully." in captured.out
    assert dummy.calls == [("db_new", True)]