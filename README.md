# Mini DWH with DuckDB, dbt and Dagster

This repository provides a tiny data warehouse setup using DuckDB for storage,
dbt for transformations and Dagster for orchestration.

## Quick start

1. Install [Docker](https://docs.docker.com/get-docker/).
2. Generate the `poetry.lock` file:

   ```bash
   poetry lock
   ```

3. Build and run the stack:

   ```bash
   docker compose up --build
   ```

4. Access the running services:
   - Dagster UI: <http://localhost:3000>
   - dbt docs: <http://localhost:8081>

The warehouse database is stored in `data/warehouse.duckdb`. Open this file in
[DBeaver](https://dbeaver.io/) to explore tables created by dbt. Two sample
sources are included: hourly commodity prices from Yahoo Finance and hourly
weather data from the Open‑Meteo API. Commodity prices cover wheat, corn,
soybeans, crude oil and a placeholder fertilizer index. The `wheat_weather`
model joins these datasets.

## Editing models

Models live under `mini_dwh_dbt/models`. Update `pipeline_config.yml` to control
which models are executed. The Dagster container schedules and runs the active
models automatically.

Toggle a model state with:

```bash
poetry run python register_model.py <model_name> --activate   # or --deactivate
```

## Repository overview

- `dagster_pipeline.py` – Dagster job reading `pipeline_config.yml`.
- `sources/` – Python modules for fetching raw data.
- `mini_dwh_dbt/` – dbt project containing models and configuration.
  - `sources/commodities.py` downloads futures prices for wheat, corn,
    soybeans, crude oil and a fertilizer index.
  - `sources/weather.py` fetches hourly temperature observations.

Start the stack with Docker, modify dbt models and watch the pipeline run!
