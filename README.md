# DWH with DuckDB, dbt and Dagster

This repository provides a tiny data warehouse setup using DuckDB for storage,
dbt for transformations and Dagster for orchestration.

## Quick start

1. Install [Docker](https://docs.docker.com/get-docker/).
2. Generate the `poetry.lock` file:

   ```bash
   poetry lock
   ```

3. Create a directory for raw CSV files next to the project:

   ```bash
   mkdir ../external_data
   ```

4. Build and run the stack:

   ```bash
   docker compose up --build
   ```

5. Access the running services:
   - Dagster UI: <http://localhost:3000>
   - dbt docs: <http://localhost:8081>

The warehouse database is stored in `data/warehouse.duckdb`. Raw CSV files are
written to `../external_data`. Open the database in
[DBeaver](https://dbeaver.io/) to explore tables created by dbt. Several sample
sources are included: hourly commodity prices from Yahoo Finance and hourly
weather data from the Open‑Meteo API. Additional fetchers provide stock prices,
currency exchange rates, short‑term weather forecasts and GDP figures from the
World Bank. Commodity prices cover wheat, corn, soybeans, crude oil and a
placeholder fertilizer index. The `wheat_weather` model joins the commodity and
weather observations.

## Editing models

Models live under `dbt/models`. Update `pipeline_config.yml` to control
which models are executed. The Dagster container schedules and runs the active
models automatically.

Toggle a model state with:


```bash
poetry run python register_model.py <model_name> --activate   # or --deactivate
```

### Manual runs

You can launch the pipeline manually from Dagster's UI using the **Launchpad**.
Provide a run configuration that specifies the fetcher and the dbt models to execute:

```yaml
ops:
  fetch_data:
    config:
      fetcher: sources.commodities.fetch
  run_dbt_pipeline:
    config:
      models: [wheat_weather]
```

If no run configuration is supplied, the job falls back to the values defined in
`pipeline_config.yml`.

## Repository overview

- `dagster_pipeline.py` – Dagster job reading `pipeline_config.yml`.
- `sources/` – Python modules for fetching raw data.
- `dbt/` – dbt project containing models and configuration.
  - Raw CSV files are stored one level above the project in `../external_data`.
  - `sources/commodities.py` downloads futures prices for wheat, corn,
    soybeans, crude oil and a fertilizer index.
  - `sources/weather.py` fetches hourly temperature observations.
  - `sources/stocks.py` retrieves daily stock prices for a few tickers.
  - `sources/exchange_rates.py` stores current USD exchange rates.
  - `sources/weather_forecast.py` downloads a 7‑day weather forecast.
  - `sources/world_bank.py` collects GDP data from the World Bank API.

Start the stack with Docker, modify dbt models and watch the pipeline run!
