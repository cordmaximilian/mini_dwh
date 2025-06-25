# Mini DWH with DuckDB and dbt

This repository contains a minimal example of a data warehouse using
[DuckDB](https://duckdb.org/) with transformation models managed by
[dbt](https://www.getdbt.com/). An example orchestration script runs the
data pipeline on an hourly schedule and fetches commodity prices before
each dbt run.

## Project structure

- `mini_dwh_dbt/` - dbt project containing models for bronze, silver and
  gold layers.
- `mini_dwh_dbt/seeds/raw/` - example CSV datasets loaded as seeds.
- `orchestrator.py` - scheduler that fetches commodity prices and runs
  the dbt pipeline every hour.
- `data/warehouse.duckdb` - DuckDB file created when the pipeline runs.

## Requirements

Dependencies are managed with [Poetry](https://python-poetry.org/). Create a
virtual environment (Poetry will do this automatically) and install the
dependencies:

```bash
pip install poetry  # if Poetry is not installed
poetry install
```

To run commands within the virtual environment use `poetry run`:

```bash
poetry run python orchestrator.py
```

## Running the pipeline

1. Initialize the database by running the orchestrator locally:

```bash
poetry run python orchestrator.py
```

Alternatively build and start the Docker container. The `data/` folder is
mounted so the DuckDB file is accessible on the host:

```bash
docker compose up --build
```

The script runs `dbt seed`, `dbt run` and `dbt test` once and then
schedules the same sequence to run every hour. Before each run it
downloads the latest commodity prices.

## dbt configuration

The dbt profile in `mini_dwh_dbt/profiles.yml` points to
`data/warehouse.duckdb`. You can override the path by setting the
`DUCKDB_PATH` environment variable.

```yaml
mini_dwh:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "data/warehouse.duckdb"
```
