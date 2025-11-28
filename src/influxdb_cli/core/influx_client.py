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

EXTENSIONS_WRITER_MAPPING = {
    '.csv': pd.DataFrame.to_csv,
    '.json': pd.DataFrame.to_json,
    '.xlsx': pd.DataFrame.to_excel,
    '.parquet': pd.DataFrame.to_parquet,
    '.feather': pd.DataFrame.to_feather
}

def file_reader(file_path: str) -> pd.DataFrame:
    file_path = Path(file_path)
    if not file_path.exists() or not file_path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")
    extension = file_path.suffix.lower()
    if extension not in EXTENSIONS_READER_MAPPING:
        raise ValueError(f"Unsupported file extension: {extension}")
    return EXTENSIONS_READER_MAPPING[extension](file_path)

def file_writer(df: pd.DataFrame, file_path: str) -> None:
    file_path = Path(file_path)
    extension = file_path.suffix.lower()
    if extension not in EXTENSIONS_WRITER_MAPPING:
        raise ValueError(f"Unsupported file extension: {extension}")
    EXTENSIONS_WRITER_MAPPING[extension](df, file_path)
    return

def timestamp_passer(timestamp: str) -> str:
    rfc3339_pattern = "%Y-%m-%dT%H:%M:%SZ"
    supported_patterns = [
        rfc3339_pattern,
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d %H:%M:%S.%f"
    ]
    for pattern in supported_patterns:
        if is_valid_timestamp(timestamp, pattern):
            return pd.to_datetime(timestamp).strftime(rfc3339_pattern)
    raise ValueError(f"Timestamp '{timestamp}' does not match any supported format.")

def is_valid_timestamp(timestamp: str, pattern: str) -> bool:
    try:
        pd.to_datetime(timestamp, format=pattern)
        return True
    except (ValueError, TypeError):
        return False


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
                "replication": rp["replicaN"],
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
            database: str,
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
            self.query(f"ALTER RETENTION POLICY {retention_policy_name} ON {database} {query_str}")
        return

    def switch_database(self, database_name: str):
        super().switch_database(database_name)
        self.config.database = database_name
        return

    def show_measurements(self) -> list[str]:
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
        if type(data.index) != pd.DatetimeIndex:
            # TODO: fix format issue
            start_ts = pd.Timestamp.now().strftime("%Y-%m-%dT%H:%M:%SZ")
            data.index = pd.date_range(start=start_ts, periods=len(data), freq='ms', tz="UTC")
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

    def delete_measurement(self, measurement_name: str, database_name: str | None = None):
        prev_db = self.config.database
        self.switch_database(database_name)
        self.query(f"DROP MEASUREMENT {measurement_name}")
        self.switch_database(prev_db)
        return

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

    def show_measurement(
            self,
            measurement_name: str,
            database_name: str,
            retention_policy: str | None = None,
            column_names: str | list[str] | None = None,
            from_time: str | None = None,
            to_time: str | None = None,
            where_clause: str | None = None,
            limit: int | None = None,
            path: str | None = None
    ) -> pd.DataFrame | int:
        prev_db = self.config.database
        try:
            from_time = timestamp_passer(from_time) if from_time else None
            to_time = timestamp_passer(to_time) if to_time else None

            self.switch_database(database_name)

            if isinstance(column_names, str):
                column_names = [column_names]

            select_clause = ", ".join(column_names) if column_names else "*"

            from_clause = f"{retention_policy}.{measurement_name}" if retention_policy else measurement_name

            conditions = []
            if from_time:
                conditions.append(f"time >= '{from_time}'")
            if to_time:
                conditions.append(f"time <= '{to_time}'")
            if where_clause:
                conditions.append(where_clause)

            where_clause_str = f" WHERE {' AND '.join(conditions)}" if conditions else ""
            limit_clause = f" LIMIT {limit}" if limit else ""

            query = f"SELECT {select_clause} FROM {from_clause}{where_clause_str}{limit_clause}"

            result = self.query(query)
            df_result = pd.DataFrame(result[measurement_name])
            if path:
                file_writer(df_result, path)
                return len(df_result)
            return pd.DataFrame(result[measurement_name])
        finally:
            self.switch_database(prev_db)

    def clean_database(self, database_name: str, exclude_measurements: list[str] | None = None):
        prev_db = self.config.database
        self.switch_database(database_name)
        measurements = self.show_measurements()
        for measurement in measurements:
            if exclude_measurements and measurement in exclude_measurements:
                continue
            self.query(f"DROP MEASUREMENT {measurement}")
        self.switch_database(prev_db)
        return
