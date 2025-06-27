import os
from dagster import Definitions, ScheduleDefinition, job, op, Field, Noneable
from utils import DBT_DIR, _run_dbt, load_config, active_models, invoke_fetcher

os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))


@op(
    config_schema={"fetcher": Field(str, is_required=False)},
)
def fetch_data(context) -> bool:
    """Import and execute the configured fetch function."""
    fetcher = context.op_config.get("fetcher")
    if not fetcher:
        cfg = load_config()
        if not cfg.get("sources"):
            raise ValueError("No sources configured in pipeline_config.yml")
        fetcher = cfg["sources"][0]["fetcher"]
        context.log.info("Using default fetcher '%s'", fetcher)
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
        cfg = load_config()
        models = list(active_models(cfg))
        context.log.info("Using active models from config: %s", models)
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
    active = active_models(cfg)
    schedules: list[ScheduleDefinition] = []
    for source in cfg.get("sources", []):
        fetcher = source["fetcher"]
        models = [m for m in source.get("models", []) if m in active]
        cron = parse_cron(source.get("schedule", "hourly"))
        schedules.append(
            ScheduleDefinition(
                job=pipeline_job,
                cron_schedule=cron,
                name=f"{source.get('name', 'source')}_schedule",
                run_config={
                    "ops": {
                        "fetch_data": {"config": {"fetcher": fetcher}},
                        "run_dbt_pipeline": {"config": {"models": models}},
                    }
                },
            )
        )
    return schedules


defs = Definitions(jobs=[pipeline_job], schedules=create_schedules())
