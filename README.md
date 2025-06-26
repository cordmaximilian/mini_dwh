# DWH with DuckDB, dbt and Dagster

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
   - Lightdash: <http://localhost:8080>

The warehouse database is stored in `data/warehouse.duckdb`. Raw CSV files are
written to `dbt/seeds/external`. Open the database in
[DBeaver](https://dbeaver.io/) to explore tables created by dbt. Several sample
sources are included: hourly commodity prices from Yahoo Finance and hourly
weather data from the Open‑Meteo API. Additional fetchers provide stock prices,
currency exchange rates, short‑term weather forecasts and GDP figures from the
World Bank. Commodity prices cover wheat, corn, soybeans, crude oil and a
placeholder fertilizer index. The `wheat_weather` model joins the commodity and
weather observations.

## Development workflow

The typical workflow when extending the warehouse is:

1. **Pull raw data**
   - Implement a new module under `sources/` exposing a `fetch()` function.
  - Run the fetcher directly to download data into `dbt/seeds/external`:

     ```bash
     poetry run python -m sources.your_source
     ```

2. **Transform with dbt**
   - Create or update models in `dbt/models`.
   - Execute them locally using:

    ```bash
    cd dbt
    dbt seed
    dbt run -s your_model
    dbt test -s your_model
    ```

   ``dbt seed`` reads the CSV files produced by your fetchers from
   ``../external_data``. The resulting tables are stored in
   ``data/warehouse.duckdb``.

3. **Automate the pipeline**
   - Register active models using `register_model.py` and edit
     `pipeline_config.yml` to add the fetcher, desired models and schedule.

     ```bash
     poetry run python register_model.py your_model --activate
     ```

   - Start the stack and Dagster will execute the job automatically according
     to the configured schedules:

     ```bash
     docker compose up
     ```

For quick local iterations you can combine both steps using `run_pipeline.py`:

```bash
poetry run python run_pipeline.py --fetcher sources.weather.fetch --models your_model
```

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
- `run_pipeline.py` – helper script to fetch data and run dbt once.
- `sources/` – Python modules for fetching raw data.
- `dbt/` – dbt project containing models and configuration.
  - Raw CSV files are stored under `dbt/seeds/external`.
  - `sources/commodities.py` downloads futures prices for wheat, corn,
    soybeans, crude oil and a fertilizer index.
  - `sources/weather.py` fetches hourly temperature observations.
  - `sources/stocks.py` retrieves daily stock prices for a few tickers.
  - `sources/exchange_rates.py` stores current USD exchange rates.
- `sources/weather_forecast.py` downloads a 7‑day weather forecast.
- `sources/world_bank.py` collects GDP data from the World Bank API.

## Data visualization with Lightdash

Lightdash reads the dbt project and lets you define metrics and dashboards as
YAML files alongside your models. The service runs on <http://localhost:8080>.
Create an account when prompted and start exploring the warehouse.

Start the stack with Docker, modify dbt models and watch the pipeline run!

## Database cleanup

Old tables may accumulate in `data/warehouse.duckdb` when models are renamed or removed.
Run the `cleanup_duckdb.py` helper to drop any tables that are not backed by a
dbt model or seed:

```bash
python cleanup_duckdb.py
```
