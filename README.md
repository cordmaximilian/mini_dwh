# Mini DWH with DuckDB and dbt

This repository contains a minimal example of a data warehouse using
[DuckDB](https://duckdb.org/) with transformation models managed by
[dbt](https://www.getdbt.com/). An example orchestration script runs the
data pipeline on a daily schedule.

## Project structure

- `mini_dwh_dbt/` - dbt project containing models for bronze, silver and
  gold layers.
- `mini_dwh_dbt/seeds/raw/` - example CSV datasets loaded as seeds.
- `orchestrator.py` - simple scheduler that executes the dbt pipeline
  every day.
- `data/warehouse.duckdb` - DuckDB file created when the pipeline runs.

## Requirements

Install dependencies with pip:

```bash
pip install duckdb dbt-core schedule
```

## Running the pipeline

1. Initialize the database by running the orchestrator:

```bash
python orchestrator.py
```

The script runs `dbt seed`, `dbt run` and `dbt test` once and then
schedules the same sequence to run daily at midnight.

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
