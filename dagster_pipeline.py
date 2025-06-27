import os
from dagster import Definitions, ScheduleDefinition, job, op, Field, Noneable
from utils import DBT_DIR, _run_dbt, invoke_fetcher

os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))


DEFAULT_FETCHER = "sources.basketball.fetch"
DEFAULT_MODELS = ["player_stats", "player_efficiency"]


@op(
    config_schema={"fetcher": Field(str, is_required=False)},
)
def fetch_data(context) -> bool:
    """Import and execute the configured fetch function."""
    fetcher = context.op_config.get("fetcher")
    if not fetcher:
        fetcher = os.environ.get("FETCHER", DEFAULT_FETCHER)
        context.log.info("Using fetcher '%s'", fetcher)
    try:
        invoke_fetcher(fetcher)
    except ValueError as exc:
        if str(exc) == f"Invalid fetcher '{fetcher}'. Expected 'module.func'":
            raise ValueError(
                f"Invalid fetcher '{fetcher}'. Expected dotted path like 'module.func'"
            ) from exc
        raise
    return True


@op(
    config_schema={"models": Field(Noneable([str]), is_required=False, default_value=None)}
)
def run_dbt_pipeline(context, _start: bool) -> None:
    """Run dbt for the configured models."""
    models = context.op_config.get("models")
    if models is None:
        env_models = os.environ.get("MODELS")
        if env_models:
            models = [m.strip() for m in env_models.split(",") if m.strip()]
        else:
            models = DEFAULT_MODELS
        context.log.info("Using models: %s", models)
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


schedule = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule=parse_cron(os.environ.get("SCHEDULE", "daily")),
    name="pipeline_schedule",
)


defs = Definitions(jobs=[pipeline_job], schedules=[schedule])
