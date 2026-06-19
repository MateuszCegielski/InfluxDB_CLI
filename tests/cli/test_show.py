import types
import pandas as pd
from typer.testing import CliRunner
import influxdb_cli.cli.commands.show as show_module

runner = CliRunner()


class DummyClient:
    def __init__(self):
        self.config = types.SimpleNamespace(database="db_current")
        self.calls = []

    def execute_method_on_db(self, method, database=None, **kwargs):
        self.calls.append((method, database, kwargs))
        if method == self.show_measurement:
            return pd.DataFrame([{"time": "2026-05-15T10:00:00Z", "value": 1}])
        if method == self.get_retention_policy_table:
            return "TABLE"
        if method == self.show_measurements:
            return ["m1", "m2"]
        return None

    def show_measurements(self):
        return ["m1", "m2"]

    def show_measurement(self, **kwargs):
        return pd.DataFrame([{"time": "2026-05-15T10:00:00Z", "value": 1}])

    def list_databases(self):
        return ["db1", "db2"]

    def get_retention_policy_table(self):
        return "TABLE"


def test_show_help():
    result = runner.invoke(show_module.app, ["--help"])
    assert result.exit_code == 0
    assert "measurement" in result.stdout
    assert "measurements" in result.stdout
    assert "databases" in result.stdout


def test_show_measurements_direct(monkeypatch, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(show_module, "InfluxClient", lambda: dummy)

    show_module.show_measurements(database_name="db1")

    captured = capsys.readouterr()
    assert "Measurements in database: db1:" in captured.out
    assert "- m1" in captured.out
    assert "- m2" in captured.out


def test_show_measurement_direct(monkeypatch, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(show_module, "InfluxClient", lambda: dummy)

    show_module.show_measurement(
        measurement_name="m1",
        retention_policy=None,
        column_names=None,
        from_time=None,
        to_time=None,
        where_clause=None,
        limit=None,
        database_name="db1",
        path=None,
    )

    captured = capsys.readouterr()
    assert "Displayed command result from measurement 'm1':" in captured.out
    assert "value" in captured.out


def test_show_database_direct(monkeypatch, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(show_module, "InfluxClient", lambda: dummy)

    show_module.show_used_db()
    captured = capsys.readouterr()
    assert "Database in use: db_current" in captured.out


def test_show_databases_direct(monkeypatch, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(show_module, "InfluxClient", lambda: dummy)

    show_module.show_databases()
    captured = capsys.readouterr()
    assert "Databases:" in captured.out
    assert "- db1" in captured.out
    assert "- db2" in captured.out