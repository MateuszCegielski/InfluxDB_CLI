import types
from pathlib import Path
from typer.testing import CliRunner
import influxdb_cli.cli.commands.add as add_module

runner = CliRunner()


class DummyClient:
    def __init__(self):
        self.config = types.SimpleNamespace(database="db_current")
        self.calls = []

    def execute_method_on_db(self, method, database=None, **kwargs):
        self.calls.append(("execute_method_on_db", method, database, kwargs))
        return 3

    def add_measurements(self, **kwargs):
        return 3

    def add_measurement_from_dir(self, **kwargs):
        self.calls.append(("add_measurement_from_dir", kwargs))
        return None

    def add_first_timestamp_to_batch_measurement(self, **kwargs):
        return "ok"


def test_add_help():
    result = runner.invoke(add_module.app, ["--help"])
    assert result.exit_code == 0
    assert "measurement" in result.stdout
    assert "measurements" in result.stdout
    assert "batch-timestamp" in result.stdout


def test_add_measurement_direct_ok(monkeypatch, tmp_path: Path, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(add_module, "InfluxClient", lambda: dummy)

    f = tmp_path / "m.csv"
    f.write_text("a\n1\n", encoding="utf-8")

    add_module.add_measurement(
        measurement_name="m1",
        path=str(f),
        database_name="db1",
        add_batch_timestamp=True,
    )

    captured = capsys.readouterr()
    assert "Added 3 measurements to database: db1." in captured.out
    assert len(dummy.calls) == 1
    _, method, database, kwargs = dummy.calls[0]
    assert method == dummy.add_measurements
    assert database == "db1"
    assert kwargs["measurement_name"] == "m1"
    assert kwargs["file_path"] == str(f)
    assert kwargs["add_batch_timestamp"] is True


def test_add_measurement_direct_invalid_path(monkeypatch, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(add_module, "InfluxClient", lambda: dummy)

    add_module.add_measurement(
        measurement_name="m1",
        path="C:/does/not/exist.csv",
        database_name=None,
        add_batch_timestamp=False,
    )

    captured = capsys.readouterr()
    assert "Error: Provided path is not a file." in captured.out
    assert dummy.calls == []


def test_add_measurements_from_dir_direct_ok(monkeypatch, tmp_path: Path, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(add_module, "InfluxClient", lambda: dummy)

    d = tmp_path / "data"
    d.mkdir()

    add_module.add_measurements(
        measurement_name="m_batch",
        path=str(d),
        add_batch_timestamp=False,
    )

    captured = capsys.readouterr()
    assert f"Created databases and added measurements from directory: {d}." in captured.out
    assert len(dummy.calls) == 1
    assert dummy.calls[0][0] == "add_measurement_from_dir"
    assert dummy.calls[0][1]["file_path"] == str(d)


def test_add_batch_timestamp_direct(monkeypatch, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(add_module, "InfluxClient", lambda: dummy)

    def fake_exec(method, database=None, **kwargs):
        dummy.calls.append(("execute_method_on_db", method, database, kwargs))
        return "Added batch timestamp"
    dummy.execute_method_on_db = fake_exec

    add_module.add_batch_timestamp(
        measurement_name="m1",
        batch_timestamp_measurement_name="batch_ts",
        database_name="db1",
    )

    captured = capsys.readouterr()
    assert "Added batch timestamp" in captured.out
    assert len(dummy.calls) == 1
    _, method, database, kwargs = dummy.calls[0]
    assert method == dummy.add_first_timestamp_to_batch_measurement
    assert database == "db1"
    assert kwargs["measurement_name"] == "m1"
    assert kwargs["batch_measurement_name"] == "batch_ts"