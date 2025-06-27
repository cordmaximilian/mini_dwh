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




def parse_cron(expr: str) -> str:
    s = str(expr).lower()
    if s == "hourly":
        return "0 * * * *"
    if s == "daily":
        return "0 0 * * *"
    if s == "weekly":
        return "0 0 * * 0"
    if s.startswith("every"):
        parts = s.split()
        if len(parts) >= 3:
            interval = int(parts[1])
            unit = parts[2].rstrip("s")
            if unit == "hour":
                return f"0 */{interval} * * *"
            if unit == "day":
                return f"0 0 */{interval} * *"
        raise ValueError(f"Invalid schedule format: {expr}")
    hour, minute = map(int, s.split(":"))
    return f"{minute} {hour} * * *"


def create_schedules():
    cfg = load_config()
    schedules: list[ScheduleDefinition] = []
    for source in cfg.get("sources", []):
        cron = parse_cron(source.get("schedule", "hourly"))
        schedules.append(
            ScheduleDefinition(
                job=pipeline_job,
                cron_schedule=cron,
                name=f"{source.get('name', 'source')}_schedule",
            )
        )
    return schedules


defs = Definitions(jobs=[pipeline_job], schedules=create_schedules())
