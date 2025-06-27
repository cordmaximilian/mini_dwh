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

   Sample seed files are included under `dbt/seeds/external` so the docs and
   Superset containers can start without fetching data. Run the fetcher scripts
   described below to refresh these CSVs with real data.

4. Access the running services:

   - Dagster UI: <http://localhost:3000>
   - dbt docs: <http://localhost:8081>
   - Superset: <http://localhost:8080>

The warehouse database is stored in `data/warehouse.duckdb`. Raw CSV files are
stored locally under `dbt/seeds/external`. Open the database in
[DBeaver](https://dbeaver.io/) to explore tables created by dbt. Models are
grouped into `bronze`, `silver` and `gold` schemas rather than having the stage
as part of the table name. Several sample
The example pipeline now focuses on basketball statistics. A single fetcher
downloads NBA season averages from the free `balldontlie` API and stores them
as CSV files. dbt models calculate per player metrics such as an efficiency
score derived from those season averages.

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

For quick local iterations run the fetcher and dbt commands manually:

```bash
poetry run python fetch_seeds.py
cd dbt && dbt seed && dbt run -s your_model && dbt test -s your_model
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
      fetcher: sources.basketball.fetch
  run_dbt_pipeline:
    config:
      models: [player_efficiency]
```

If no run configuration is supplied, the job falls back to the values defined in
`pipeline_config.yml`.

## Repository overview

- `dagster_pipeline.py` – Dagster job reading `pipeline_config.yml`.
- `fetch_seeds.py` – simple script to download raw data into `dbt/seeds/external`.
- `sources/` – Python modules for fetching raw data.
- `dbt/` – dbt project containing models and configuration.
  - Raw CSV files are stored under `dbt/seeds/external`.
  - `sources/basketball.py` downloads NBA season averages.

## Data visualization with Superset

Superset provides an intuitive interface for exploring your data. The service
runs on <http://localhost:8080>. The compose file bootstraps an administrator
account by executing ``superset fab create-admin`` when the container starts.
By default the credentials are ``max`` / ``admin``. You can change them through
the environment variables defined in ``docker-compose.yml``. After logging in
you may change the password or add more users from Superset's
**Settings → List Users** menu.

CSV files and Superset assets are stored locally as part of the Docker volumes.


Start the stack with Docker, modify dbt models and watch the pipeline run!

## Database cleanup

Old tables may accumulate in `data/warehouse.duckdb` when models are renamed or removed.
Run the `cleanup_duckdb.py` helper to drop any tables that are not backed by a
dbt model or seed:

```bash
python cleanup_duckdb.py
```
