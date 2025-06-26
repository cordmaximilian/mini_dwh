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
- `mini_dwh_dbt/` is used as the working directory for all dbt commands
  executed by the orchestrator and Prefect flow.
- `data/warehouse.duckdb` - DuckDB file created when the pipeline runs.

## Requirements

Dependencies are managed with [Poetry](https://python-poetry.org/). Create a
virtual environment (Poetry will do this automatically) and install the
dependencies (including `pandas` and `yfinance` used to fetch commodity
prices):

```bash
pip install poetry  # if Poetry is not installed
poetry install
```

To run commands within the virtual environment use `poetry run`:


```bash
poetry run python orchestrator.py
```

## Running dbt commands manually

When invoking `dbt` yourself (outside of the orchestrator or Prefect flow)
make sure the CLI is executed in the `mini_dwh_dbt/` directory so that the
`dbt_project.yml` file is discovered. For example, to run a single model:

```bash
cd mini_dwh_dbt
dbt run -s models/bronze/orders_bronze.sql
```

## Running the pipeline

The simplest way to get started is to build and start the Docker
container. This spins up the orchestrator and schedules the pipeline
immediately:

```bash
docker compose up --build
```

The service mounts the `data/` directory so the DuckDB file is available on
the host. The orchestrator sets `DBT_PROFILES_DIR` automatically so dbt uses
the bundled profile. Once the container is running you can open a shell inside it if
you want to execute additional `dbt` commands or inspect the database:

```bash
docker compose exec dwh bash
```

Running the container executes `dbt seed`, `dbt run` and `dbt test` once and
then schedules the same sequence every hour. Before each run the latest
commodity prices are downloaded.

If you prefer running everything locally, execute the orchestrator with
Poetry instead:

```bash
poetry run python orchestrator.py
```

## Pipeline monitoring with Prefect

The project includes an optional Prefect flow in `prefect_flow.py` so you
can monitor pipeline runs using Prefect's UI.

Start the local Prefect server in one terminal:

```bash
poetry run prefect orion start
```

Then execute the flow in another terminal:

```bash
poetry run python prefect_flow.py
```

The Prefect UI, available at `http://127.0.0.1:4200`, shows the status of
each run so you can keep track of your pipeline executions.

## dbt configuration

The dbt profile in `mini_dwh_dbt/profiles.yml` points to
`data/warehouse.duckdb`. The orchestrator automatically sets
`DBT_PROFILES_DIR` to use this profile. You can override the database path by
setting the `DUCKDB_PATH` environment variable.

```yaml
mini_dwh:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "data/warehouse.duckdb"
```
