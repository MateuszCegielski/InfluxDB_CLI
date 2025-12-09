# InfluxDB CLI

A command-line interface for interacting with InfluxDB databases, providing an intuitive way to manage databases, measurements, and retention policies.

## Features

- **Database Management**: Create, drop, list, and switch databases
- **Measurement Operations**: Query, show, and export measurement data
- **Retention Policy Management**: Create, list, alter, and drop retention policies
- **Configuration Management**: Store and manage connection settings
- **Data Export**: Save query results to CSV, JSON, or Parquet formats
- **Interactive CLI**: Built with Typer for a modern command-line experience

## Installation

### Requirements

- Python >= 3.11
- pip

### Install from source

```bash
git clone https://github.com/MateuszCegielski/InfluxDB_CLI.git
cd InfluxDB_CLI
pip install -e .
```
## Configuration
The CLI uses a YAML configuration file (`default_config.yaml`) located in `src/influxdb_cli/config/`.

Configuration is automatically loaded from this file and includes:
- InfluxDB connection settings (host, port, username, password)
- Default database
- Retention policy settings
- The configuration is managed through the `ConfigManager` class and stored in your system's 
  application data directory.
### Usage
Database Commands:
```bash
# List all databases
influx database list

# Create a new database
influx database create my_database

# Create a database with retention policies from config
influx database create my_database --retention-policy

# Switch to a database
influx database use my_database

# Delete a database
influx database delete my_database

# Show current database
influx database show

# Clean a database (remove all data)
influx database clean --database-name my_database

# Clean a database but exclude specific measurements
influx database clean -d my_database --except "measurement1,measurement2"
```
Measurement Commands:
```bash
# Show last 10 records from a measurement
influx measurement show my_measurement -l 10

# Show specific columns
influx measurement show my_measurement -c column1,column2

# Filter by time range
influx measurement show my_measurement --from "2025-01-01T00:00:00Z" --to "2025-12-31T23:59:59Z"

# Add WHERE clause conditions
influx measurement show my_measurement -w "value > 100"

# Export to file
influx measurement show my_measurement -p output.csv
influx measurement show my_measurement -p output.json
influx measurement show my_measurement -p output.parquet
```
Retention Policy Commands:
```bash
# List retention policies for current database
influx database list-retention-policies

# List retention policies for specific database
influx database list-retention-policies --database my_database

# Modify a retention policy
influx database modify-retention-policy my_policy --duration 60d --replication 2

# Modify retention policy for specific database
influx database modify-retention-policy my_policy -d my_database -n 90d -r 1
```
## Examples
### Query measurement with filters.
```bash
# Get data from last hour with specific value threshold
influx measurement show sensor_data \
  --from "2025-11-26T12:00:00Z" \
  --to "2025-11-26T13:00:00Z" \
  -w "temperature > 20" \
  -l 100
```
### Export data to diffrent formats.
```bash
# Export to CSV
influx measurement show sensor_data -l 1000 -p /path/to/data.csv

# Export to JSON
influx measurement show sensor_data -l 1000 -p /path/to/data.json

# Export to Parquet
influx measurement show sensor_data -l 1000 -p /path/to/data.parquet
```
### Database management workflow.
```bash
# Create database with retention policies
influx database create production_db --retention-policy

# Switch to it
influx database use production_db

# List retention policies
influx database list-retention-policies

# Modify retention policy
influx database modify-retention-policy default --duration 90d

# Clean database except specific measurements
influx database clean -d production_db --except "important_data,backup_data"
```
## Project Structure
```
InfluxDBCLI/
├── src/
│   └── influxdb_cli/
│       ├── cli/
│       │   ├── commands/
│       │   │   ├── config.py          # Configuration commands
│       │   │   ├── database.py        # Database commands
│       │   │   └── measurement.py     # Measurement commands
│       │   └── main.py                 # CLI entry point
│       ├── config/
│       │   ├── config_manager.py       # Configuration management
│       │   └── default_config.yaml     # Default configuration file
│       └── core/
│           └── influx_client.py        # InfluxDB client wrapper
├── tests/
├── pyproject.toml
├── requirements.txt
└── README.md
```
## Dependencies
- typer - CLI framework
- influxdb - InfluxDB Python client
- pandas - Data manipulation
- pyarrow - Parquet file support
- pyyaml - YAML configuration parsing
- pydantic - Data validation
- platformdirs - Cross-platform config directories