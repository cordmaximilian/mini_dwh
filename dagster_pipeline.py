import os
from dagster import Definitions, ScheduleDefinition, job, op, Field, Noneable
from utils import DBT_DIR, _run_dbt, load_config
from sources.basketball import fetch

# Default dbt models to execute
MODELS = [
    "player_stats",
    "player_efficiency",
]

os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))


@op
def fetch_data() -> bool:
    """Fetch basketball data."""
    fetch()
    return True


@op(
    config_schema={"models": Field(Noneable([str]), is_required=False, default_value=None)}
)
def run_dbt_pipeline(context, _start: bool) -> None:
    """Run dbt for the configured models."""
    models = context.op_config.get("models")
    if models is None:
        models = MODELS
        context.log.info("Using default models: %s", models)
    _run_dbt(["seed"])
    if models:
        _run_dbt(["run", "-s", *models])
        _run_dbt(["test", "-s", *models])
    else:
        _run_dbt(["run"])
        _run_dbt(["test"])


@job
def pipeline_job() -> None:
    """Dagster job that fetches data and executes dbt."""
    run_dbt_pipeline(fetch_data())

DEFAULT_CRON = "0 0 * * *"


def create_schedules():
    cfg = load_config()
    schedules: list[ScheduleDefinition] = []
    for source in cfg.get("sources", []):
        fetcher = source["fetcher"]
        models = [m for m in source.get("models", []) if m in active]
        cron = source.get("schedule", DEFAULT_CRON)

        schedules.append(
            ScheduleDefinition(
                job=pipeline_job,
                cron_schedule=cron,
                name=f"{source.get('name', 'source')}_schedule",
            )
        )
    return schedules


defs = Definitions(jobs=[pipeline_job], schedules=create_schedules())
