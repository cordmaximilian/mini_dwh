import os
from dagster import Definitions, ScheduleDefinition, job, op, Field, Noneable
from utils import DBT_DIR, _run_dbt, invoke_fetcher

BASKETBALL_FETCHER = "sources.basketball.fetch"
FINANCE_FETCHER = "sources.finance.fetch"

BASKETBALL_MODELS = [
    "player_stats",
    "players",
    "teams",
    "games",
    "game_stats",
    "player_efficiency",
    "player_game_facts",
]

FINANCE_MODELS = [
    "stock_prices",
    "commodity_prices",
    "weather",
    "news",
    "daily_market_data",
]
DEFAULT_CRON = "0 0 * * *"


def _get_models(env: str, default: list[str]) -> list[str]:
    models_str = os.getenv(env)
    if models_str:
        return [m.strip() for m in models_str.split(",") if m.strip()]
    return default


os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))


def make_fetch_op(fetcher: str, name: str):
    @op(name=name)
    def _fetch_data() -> bool:
        invoke_fetcher(fetcher)
        return True

    return _fetch_data


def make_dbt_op(env_var: str, default_models: list[str], name: str):
    @op(name=name, config_schema={"models": Field(Noneable([str]), is_required=False, default_value=None)})
    def _run_dbt_pipeline(context, _start: bool) -> None:
        models = context.op_config.get("models")
        if models is None:
            models = _get_models(env_var, default_models)
            context.log.info("Using default models: %s", models)
        _run_dbt(["seed"])
        if models:
            _run_dbt(["run", "-s", *models])
            _run_dbt(["test", "-s", *models])
        else:
            _run_dbt(["run"])
            _run_dbt(["test"])

    return _run_dbt_pipeline


basketball_fetch = make_fetch_op(BASKETBALL_FETCHER, "basketball_fetch")
finance_fetch = make_fetch_op(FINANCE_FETCHER, "finance_fetch")

basketball_dbt = make_dbt_op("BASKETBALL_MODELS", BASKETBALL_MODELS, "basketball_dbt")
finance_dbt = make_dbt_op("FINANCE_MODELS", FINANCE_MODELS, "finance_dbt")


@job
def basketball_job() -> None:
    """Pipeline for basketball related models."""
    basketball_dbt(basketball_fetch())


@job
def finance_job() -> None:
    """Pipeline for finance related models."""
    finance_dbt(finance_fetch())

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


basketball_schedule = ScheduleDefinition(
    job=basketball_job,
    cron_schedule=parse_cron(os.getenv("BASKETBALL_SCHEDULE", DEFAULT_CRON)),
    name="basketball_schedule",
)

finance_schedule = ScheduleDefinition(
    job=finance_job,
    cron_schedule=parse_cron(os.getenv("FINANCE_SCHEDULE", DEFAULT_CRON)),
    name="finance_schedule",
)

defs = Definitions(
    jobs=[basketball_job, finance_job],
    schedules=[basketball_schedule, finance_schedule],
)
