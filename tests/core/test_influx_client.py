import types
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

import influxdb_cli.core.influx_client as ic


def test_is_valid_timestamp_true():
    assert ic.is_valid_timestamp("2026-05-15 12:00:00", "%Y-%m-%d %H:%M:%S") is True


def test_is_valid_timestamp_false():
    assert ic.is_valid_timestamp("not-a-date", "%Y-%m-%d %H:%M:%S") is False


@pytest.mark.parametrize(
    "value",
    [
        "2026-05-15 12:00:00",
        "2026-05-15T12:00:00Z",
        "2026-05-15T12:00:00.123456Z",
        "2026-05-15 12:00:00.123456",
    ],
)
def test_timestamp_passer_supported_formats(value):
    out = ic.timestamp_passer(value)
    # Expect RFC3339 with microseconds and Z suffix
    assert out.endswith("Z")
    assert "T" in out
    assert len(out) >= 20


def test_timestamp_passer_raises_for_invalid():
    with pytest.raises(ValueError, match="does not match any supported format"):
        ic.timestamp_passer("invalid-ts")


def test_file_reader_raises_for_missing_file(tmp_path: Path):
    missing = tmp_path / "nope.csv"
    with pytest.raises(FileNotFoundError, match="File not found"):
        ic.file_reader(str(missing))


def test_file_reader_raises_for_unsupported_extension(tmp_path: Path):
    f = tmp_path / "data.unsupported"
    f.write_text("x", encoding="utf-8")
    with pytest.raises(ValueError, match="Unsupported file extension"):
        ic.file_reader(str(f))


def test_file_reader_dispatches_csv(tmp_path: Path):
    f = tmp_path / "data.csv"
    f.write_text("a\n1\n", encoding="utf-8")
    df = ic.file_reader(str(f))
    assert list(df.columns) == ["a"]
    assert len(df) == 1


def test_file_writer_raises_for_unsupported_extension(tmp_path: Path):
    df = pd.DataFrame({"a": [1]})
    out = tmp_path / "x.unsupported"
    with pytest.raises(ValueError, match="Unsupported file extension"):
        ic.file_writer(df, str(out))


def test_file_writer_csv(tmp_path: Path):
    df = pd.DataFrame({"a": [1, 2]})
    out = tmp_path / "out.csv"
    ic.file_writer(df, str(out))
    assert out.exists()


# ----------------------------
# InfluxClient method tests
# (without real DB)
# ----------------------------

@pytest.fixture
def client():
    c = object.__new__(ic.InfluxClient)
    c.config = types.SimpleNamespace(
        database="db1",
        host="localhost",
        port=8086,
        retention_policies=[
            {
                "name": "rp_7d",
                "duration": "7d",
                "replication": 1,
                "shard_duration": "1h",
                "default": True,
            }
        ],
    )
    c.query = MagicMock()
    c.switch_database = MagicMock(side_effect=lambda db: setattr(c.config, "database", db))
    c.get_list_database = MagicMock(return_value=[{"name": "db1"}, {"name": "db2"}])
    c.get_list_measurements = MagicMock(return_value=[{"name": "m1"}, {"name": "m2"}, {"name": "m3"}])
    c.get_list_retention_policies = MagicMock(
        return_value=[
            {
                "name": "rp_7d",
                "duration": "168h0m0s",
                "shardGroupDuration": "1h0m0s",
                "replicaN": 1,
                "default": True,
            }
        ]
    )
    c.write_points = MagicMock()
    return c


def test_is_valid_database(client):
    assert client.is_valid_database("db1") is True
    assert client.is_valid_database("missing") is False


def test_ensure_database_exists_ok(client):
    client.ensure_database_exists("db1")


def test_ensure_database_exists_raises(client):
    with pytest.raises(ValueError, match="does not exist"):
        client.ensure_database_exists("missing")


def test_ensure_database_not_exists_ok(client):
    client.ensure_database_not_exists("new_db")


def test_ensure_database_not_exists_raises(client):
    with pytest.raises(ValueError, match="already exist"):
        client.ensure_database_not_exists("db1")


def test_execute_method_on_db_with_explicit_database(client):
    fn = MagicMock(return_value=123)
    out = client.execute_method_on_db(fn, database="db2", x=1)
    assert out == 123
    fn.assert_called_once_with(x=1)
    # switched to db2 then restored to original db1
    assert client.switch_database.call_count >= 2


def test_create_database_without_rp(client):
    # retention_policy=False still goes through because code checks None;
    # test current behavior explicitly
    client.create_database("new_db", retention_policy=None)
    client.query.assert_called_once_with("CREATE DATABASE new_db")


def test_create_database_with_rp(client):
    client.create_database("new_db", retention_policy=True)
    calls = [str(c) for c in client.query.call_args_list]
    assert any("CREATE DATABASE new_db" in c for c in calls)
    assert any("CREATE RETENTION POLICY rp_7d ON new_db" in c for c in calls)


def test_delete_database(client):
    client.delete_database("db2")
    client.query.assert_called_with("DROP DATABASE db2")


def test_list_databases_prefix(client):
    assert client.list_databases(prefix="db") == ["db1", "db2"]


def test_list_databases_raises_when_empty(client):
    client.get_list_database.return_value = [{"name": "abc"}]
    with pytest.raises(ValueError, match="No databases found"):
        client.list_databases(prefix="db")


def test_get_retention_policy_table(client):
    table = client.get_retention_policy_table()
    assert table.title.startswith("Retention Policies")
    assert len(table.columns) == 5


def test_delete_retention_policy(client):
    client.delete_retention_policy("db1", "rp_7d")
    client.query.assert_called_with("DROP RETENTION POLICY rp_7d ON db1")


def test_modify_retention_policy_no_changes(client):
    msg = client.modify_retention_policy("rp_7d")
    assert "successfully" in msg
    client.query.assert_not_called()


def test_modify_retention_policy_with_changes(client):
    client.modify_retention_policy("rp_7d", new_duration="30d", new_replication=2, set_default=True)
    q = client.query.call_args[0][0]
    assert "ALTER RETENTION POLICY rp_7d ON db1" in q
    assert "DURATION 30d" in q
    assert "REPLICATION 2" in q
    assert "DEFAULT" in q


def test_show_measurements(client):
    assert client.show_measurements() == ["m1", "m2", "m3"]


def test_delete_measurement(client):
    client.delete_measurement("m1")
    client.query.assert_called_with("DROP MEASUREMENT m1")


def test_add_measurements_sets_index_and_writes(client, monkeypatch):
    df = pd.DataFrame({"v": [1, 2, 3]})
    monkeypatch.setattr(ic, "file_reader", lambda _: df.copy())

    n = client.add_measurements(file_path="dummy.csv", measurement_name="mnew", add_batch_timestamp=False)

    assert n == 3
    assert client.write_points.call_count == 1
    kwargs = client.write_points.call_args.kwargs
    assert kwargs["measurement"] == "mnew"
    assert kwargs["database"] == "db1"
    assert isinstance(kwargs["dataframe"].index, pd.DatetimeIndex)


def test_add_measurements_uses_file_stem_when_measurement_name_missing(client, monkeypatch):
    df = pd.DataFrame({"v": [1]})
    monkeypatch.setattr(ic, "file_reader", lambda _: df.copy())
    client.add_measurements(file_path="abc.csv", measurement_name=None, add_batch_timestamp=False)
    kwargs = client.write_points.call_args.kwargs
    assert kwargs["measurement"] == "abc"


def test_show_measurement_builds_query_and_returns_dataframe(client):
    # Simulate query result like DataFrameClient returns
    ts = pd.Timestamp("2026-05-15T10:00:00Z")
    client.query.return_value = {"m1": pd.DataFrame([{"time": ts, "value": 1.2}])}

    out = client.show_measurement(
        measurement_name="m1",
        retention_policy="rp_7d",
        column_names=["value"],
        from_time="2026-05-15T09:00:00Z",
        to_time="2026-05-15T11:00:00Z",
        where_clause="value > 1",
        limit=10,
        path=None,
    )

    assert isinstance(out, pd.DataFrame)
    q = client.query.call_args[0][0]
    assert "SELECT value FROM rp_7d.m1" in q
    assert "WHERE" in q
    assert "LIMIT 10" in q


def test_show_measurement_writes_file_when_path_provided(client, monkeypatch):
    ts = pd.Timestamp("2026-05-15T10:00:00Z")
    client.query.return_value = {"m1": pd.DataFrame([{"time": ts, "value": 1.2}])}
    writer = MagicMock()
    monkeypatch.setattr(ic, "file_writer", writer)

    n = client.show_measurement(measurement_name="m1", path="out.csv")
    assert n == 1
    writer.assert_called_once()


def test_clean_database_deletes_all(client):
    msg = client.clean_database()
    assert "Deleted 3 measurement(s)" in msg
    dropped = [c.args[0] for c in client.query.call_args_list]
    assert dropped == [
        "DROP MEASUREMENT m1",
        "DROP MEASUREMENT m2",
        "DROP MEASUREMENT m3",
    ]


def test_clean_database_with_excludes(client):
    msg = client.clean_database(exclude_measurements=["m2"])
    assert "except: m2" in msg
    dropped = [c.args[0] for c in client.query.call_args_list]
    assert dropped == [
        "DROP MEASUREMENT m1",
        "DROP MEASUREMENT m3",
    ]


def test_clean_database_raises_for_missing_excluded(client):
    with pytest.raises(ValueError, match="not found in database"):
        client.clean_database(exclude_measurements=["missing"])