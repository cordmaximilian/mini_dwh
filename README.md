# Mini DWH with PostgreSQL and dbt

This repository contains a minimal example of a data warehouse using
[PostgreSQL](https://www.postgresql.org/) with transformation models managed by
[dbt](https://www.getdbt.com/). An orchestration script reads
`pipeline_config.yml` at startup and schedules each data source
individually. Before running the selected dbt models the configured fetch
function is executed for that source.

## Project structure

- `mini_dwh_dbt/` - dbt project containing models for bronze, silver and
  gold layers.
- `sources/` - Python modules that download external datasets. Each module
  implements a `fetch()` function.
- `mini_dwh_dbt/seeds/raw/` - example CSV datasets loaded as seeds.
- `orchestrator.py` - scheduler that loads `pipeline_config.yml` and
  executes each source on its defined schedule.
- `mini_dwh_dbt/` is used as the working directory for all dbt commands
  executed by the orchestrator and Prefect flow.

## Configuration

`pipeline_config.yml` controls which dbt models are run and how each source is executed. It contains two top-level sections:

- `models` – list of dbt models with an `active` flag.
- `sources` – entries defining the fetcher function, schedule and models to run.

Example:

```yaml
models:
  - name: orders_enriched
    active: true
  - name: sales_by_country
    active: true

sources:
  - name: commodities
    fetcher: sources.commodities.fetch
    schedule: "hourly"
    models:
      - orders_enriched
      - sales_by_country
```
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
container. This spins up the orchestrator which loads
`pipeline_config.yml` and schedules the configured sources immediately:

```bash
docker compose up --build
```

The Compose stack exposes two services: the orchestrator container and a
PostgreSQL server accessible on `localhost:5432`. The orchestrator sets
`DBT_PROFILES_DIR` automatically so dbt uses the bundled profile. Once the
containers are running you can open a shell inside the DWH service if you
want to execute additional `dbt` commands or inspect the database:

```bash
docker compose exec dwh bash
```

Running the container executes each source once and then continues to run
them based on the schedule defined in `pipeline_config.yml`.
Errors during fetching or model execution are logged. The orchestrator
continues scheduling other runs so the container stays alive even if a
step fails.

If you prefer running everything locally, execute the orchestrator with
Poetry instead:

```bash
poetry run python orchestrator.py
```

## Pipeline monitoring with Prefect

The project includes an optional Prefect flow in `prefect_flow.py` so you
can monitor pipeline runs using Prefect's UI. Deployments are created from
`pipeline_config.yml` using `register_deployments.py`.

Start the local Prefect server in one terminal:

```bash
poetry run prefect orion start
```

Register the deployments in another terminal:

```bash
poetry run python register_deployments.py
```

Finally, start a worker to execute scheduled runs:

```bash
poetry run prefect agent start -q default
```

The Prefect UI, available at `http://127.0.0.1:4200`, shows the status of
each run so you can keep track of your pipeline executions.

## Database access

The PostgreSQL service persists its data in a Docker volume so the
warehouse state is retained between runs. Connect with any PostgreSQL
client using the credentials defined in `docker-compose.yml`.

## Adding new data sources

Fetcher modules live in the `sources/` package. Each module must implement a
`fetch()` function that downloads the raw data. See `sources/README.md` for
more details.

To register a new source create a module and update `pipeline_config.yml`:

```yaml
sources:
  - name: my_source
    fetcher: sources.my_source.fetch
    schedule: "daily"
    models:
      - my_model
```

If the pipeline should run a new dbt model, activate it with `register_model.py`:

```bash
poetry run python register_model.py my_model --activate
```

## dbt configuration

The dbt profile in `mini_dwh_dbt/profiles.yml` is used by the orchestrator.
It automatically sets
`DBT_PROFILES_DIR` to use this profile. Connection details are configured via
environment variables passed to the container.

```yaml
mini_dwh:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST') }}"
      port: "{{ env_var('POSTGRES_PORT') }}"
      user: "{{ env_var('POSTGRES_USER') }}"
      pass: "{{ env_var('POSTGRES_PASSWORD') }}"
      dbname: "{{ env_var('POSTGRES_DB') }}"
      schema: public
```
