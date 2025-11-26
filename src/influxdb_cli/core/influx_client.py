from pathlib import Path

from influxdb import DataFrameClient
from influxdb_cli.config.config_manager import load_config, save_config
import pandas as pd


EXTENSIONS_READER_MAPPING = {
    '.csv': pd.read_csv,
    '.json': pd.read_json,
    '.xlsx': pd.read_excel,
    '.parquet': pd.read_parquet,
    '.feather': pd.read_feather


}

def file_reader(file_path: str) -> pd.DataFrame:
    file_path = Path(file_path)
    if not file_path.exists() or not file_path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")
    extension = file_path.suffix.lower()
    if extension not in EXTENSIONS_READER_MAPPING:
        raise ValueError(f"Unsupported file extension: {extension}")
    reader_function = EXTENSIONS_READER_MAPPING[extension]
    df = reader_function(file_path)
    return df


class InfluxClient(DataFrameClient):
    def __init__(self):
        self.config = load_config()
        super().__init__(host=self.config.host, port=self.config.port, database=self.config.database)

    def __del__(self):
        save_config(self.config)

    def is_default_rp(self, default_rp: bool) -> str:
        if default_rp:
            return "DEFAULT"
        return ""

    def create_database(self, dbname: str, retention_policy: bool = False):
        self.query(f"CREATE DATABASE {dbname}")
        if retention_policy is None:
            return

        for rp in self.config.retention_policies:
            name = rp["name"]
            duration = rp["duration"]
            replication = rp["replication"]
            shard_duration = rp["shard_duration"]
            default = rp.get("default", False)
            self.query(f"CREATE RETENTION POLICY {name} ON {dbname} DURATION {duration} "
                       f"REPLICATION {replication} SHARD DURATION {shard_duration} "
                       f"{self.is_default_rp(default)}")
        return

    def delete_database(self, dbname: str):
        self.query(f"DROP DATABASE {dbname}")
        return

    def list_databases(self) -> list[str]:
        result = self.query("SHOW DATABASES")
        databases = [db['name'] for db in result.get_points()]
        return databases

    def list_retention_policies(self, dbname: str) -> list[dict]:
        result = self.query(f"SHOW RETENTION POLICIES ON {dbname}")
        rps = []
        for rp in result.get_points():
            rps.append({
                "name": rp["name"],
                "duration": rp["duration"],
                "replication": rp["replication"],
                "shard_duration": rp["shardGroupDuration"],
                "default": rp["default"]
            })
        return rps

    def delete_retention_policy(self, dbname: str, rp_name: str):
        self.query(f"DROP RETENTION POLICY {rp_name} ON {dbname}")
        return

    def modify_retention_policy(
            self,
            retention_policy_name: str,
            new_duration: str = None,
            new_replication: int = None,
            set_default: bool = False
    ):
        query_parts = []
        if new_duration:
            query_parts.append(f"DURATION {new_duration}")
        if new_replication:
            query_parts.append(f"REPLICATION {new_replication}")
        if set_default:
            query_parts.append("DEFAULT")
        query_str = " ".join(query_parts)
        if query_str:
            self.query(f"ALTER RETENTION POLICY {retention_policy_name} {query_str}")
        return

    def switch_database(self, database_name: str):
        super().switch_database(database_name)
        self.config.database = database_name
        return

    def show_measurements(self, database_name: str | None = None ) -> list[str]:
        result = self.query("SHOW MEASUREMENTS")
        measurements = [measurement['name'] for measurement in result.get_points()]
        return measurements

    def add_measurements(
            self,
            database_name: str | None = None,
            file_path: str | None = None,
            measurement_name: str | None = None
    ):
        data = file_reader(file_path)
        if 'timestamp' not in data.columns:
            data.index = pd.date_range(start=pd.Timestamp.now(), periods=len(data), freq='ms')
        if measurement_name is None:
            measurement_name = Path(file_path).stem

        self.write_points(
            dataframe=data,
            measurement=measurement_name,
            database=database_name or self.config.database,
            time_precision='ms',
            batch_size=1000
        )
        return len(data)

    def add_measurement_from_dir(
            self,
            file_path: str | None = None,
            measurement_name: str | None = None
    ):
        if file_path is None:
            raise ValueError("Directory path must be provided.")
        dir_path = Path(file_path)
        if not dir_path.exists() or not dir_path.is_dir():
            raise FileNotFoundError(f"Directory not found: {dir_path}")
        for file in dir_path.iterdir():
            if file.is_file():
                data = file_reader(str(file))
                if 'timestamp' not in data.columns:
                    data.index = pd.date_range(start=pd.Timestamp.now(), periods=len(data), freq='ms')
                measurement = measurement_name
                self.create_database(file.stem, retention_policy=True)
                self.write_points(
                    dataframe=data,
                    measurement=measurement,
                    database=file.stem,
                    time_precision='ms',
                    batch_size=1000
                )
        return
