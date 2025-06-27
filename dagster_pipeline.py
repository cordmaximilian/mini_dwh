import os
from dagster import Definitions, ScheduleDefinition, job, op, Field, Noneable
from utils import DBT_DIR, _run_dbt, invoke_fetcher

DEFAULT_FETCHER = "sources.basketball.fetch"
DEFAULT_MODELS = [
    "player_stats",
    "players",
    "teams",
    "games",
    "game_stats",
    "player_efficiency",
    "player_game_facts",
]
DEFAULT_CRON = "0 0 * * *"


def get_fetcher() -> str:
    return os.getenv("FETCHER", DEFAULT_FETCHER)


def get_models() -> list[str]:
    models_str = os.getenv("MODELS")
    if models_str:
        return [m.strip() for m in models_str.split(",") if m.strip()]
    return DEFAULT_MODELS

os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))


@op
def fetch_data() -> bool:
    """Fetch raw data using the configured fetcher."""
    invoke_fetcher(get_fetcher())
    return True


@op(
    config_schema={"models": Field(Noneable([str]), is_required=False, default_value=None)}
)
def run_dbt_pipeline(context, _start: bool) -> None:
    """Run dbt for the configured models."""
    models = context.op_config.get("models")
    if models is None:
        models = get_models()
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
    mapping = {
        "hourly": "0 * * * *",
        "daily": "0 0 * * *",
        "weekly": "0 0 * * 0",
    }
    if expr in mapping:
        return mapping[expr]
    if expr.startswith("every "):
        num, unit = expr.split()[1:3]
        if unit.startswith("hour"):
            return f"0 */{num} * * *"
        if unit.startswith("day"):
            return f"0 0 */{num} * *"
    if ":" in expr:
        hour, minute = expr.split(":")
        return f"{minute} {hour} * * *"
    return expr


schedule = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule=parse_cron(os.getenv("SCHEDULE", DEFAULT_CRON)),
    name="pipeline_schedule",
)

defs = Definitions(jobs=[pipeline_job], schedules=[schedule])
