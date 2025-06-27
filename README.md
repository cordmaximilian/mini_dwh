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

Sample seed files are included under `dbt/seeds/external` so the docs container
can start without fetching data. Run the fetcher scripts described below to
refresh these CSVs with real data.

4. Access the running services:

   - Dagster UI: <http://localhost:3000>
   - dbt docs: <http://localhost:8081>
   - Jupyter: <http://localhost:8888>

The warehouse database is stored in `data/warehouse.duckdb`. Raw CSV files are
stored locally under `dbt/seeds/external`. Open the database in
[DBeaver](https://dbeaver.io/) to explore tables created by dbt. Models are
grouped into `bronze`, `silver` and `gold` schemas rather than having the stage
as part of the table name. Several sample
The example pipeline now focuses on basketball statistics. The fetcher
downloads player, team and game data from the free `balldontlie` API and stores
them as CSV files. dbt models build a star schema with player and team
dimensions plus game level facts. Additional models calculate metrics like
player efficiency for richer analysis.

## Development workflow

The typical workflow when extending the warehouse is:

1. **Pull raw data**
   - Implement a new module under `sources/` exposing a `fetch()` function.
   - Run `fetch_seeds.py` to download data into `dbt/seeds/external`:

     ```bash
     poetry run python fetch_seeds.py
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

   ``dbt seed`` loads the CSVs from ``dbt/seeds/external`` into ``data/warehouse.duckdb``.

3. **Automate the pipeline**
   - Configure the fetcher, dbt models and schedule using environment
     variables. For example:

     ```bash
     export FETCHER=sources.basketball.fetch
     export MODELS=player_stats,player_efficiency
     export SCHEDULE=daily
     ```

   - Start the stack and Dagster will execute the job automatically according
     to the configured schedule:

     ```bash
     docker compose up
     ```

For quick local iterations run the fetcher and dbt commands manually:

```bash
poetry run python fetch_seeds.py
cd dbt && dbt seed && dbt run -s your_model && dbt test -s your_model
```

## Editing models

Models live under `dbt/models`. Define the list of models to execute via the
`MODELS` environment variable. The Dagster container runs these models on the
configured schedule.

### Manual runs

You can launch the pipeline manually from Dagster's UI using the **Launchpad**.
Provide a run configuration that specifies the fetcher and the dbt models to execute:

```yaml
ops:
  fetch_data:
    config:
      fetcher: sources.basketball.fetch
  run_dbt_pipeline:
    config:
      models: [player_efficiency]
```

If no run configuration is supplied, the job falls back to the values provided
through environment variables.

## Repository overview

- `dagster_pipeline.py` – Dagster job reading environment variables.
- `fetch_seeds.py` – simple script to download raw data into `dbt/seeds/external`.
- `sources/` – Python modules for fetching raw data.
- `dbt/` – dbt project containing models and configuration.
  - Raw CSV files are stored under `dbt/seeds/external`.
  - `sources/basketball.py` downloads NBA season averages.

## Data visualization with Jupyter

This project ships with a Jupyter Notebook server for quick exploration. After
starting the stack you can access the notebook interface on
<http://localhost:8888>. Example notebooks live in the `notebooks/` directory
and connect directly to `data/warehouse.duckdb` using the `duckdb` Python
package. Feel free to create your own notebooks to analyze the transformed
tables and visualize results using your favorite libraries.

Start the stack with Docker, modify dbt models and watch the pipeline run!

## Database cleanup

Old tables may accumulate in `data/warehouse.duckdb` when models are renamed or removed.
Run the `cleanup_duckdb.py` helper to drop any tables that are not backed by a
dbt model or seed:

```bash
python cleanup_duckdb.py
```

## Running tests

Unit tests ensure the helper scripts behave correctly even without optional
dependencies. Install development requirements with Poetry and execute the test
suite using `pytest`:

```bash
poetry install --with dev
poetry run pytest -q
```
