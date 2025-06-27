import importlib
import os
import subprocess
import sys
from pathlib import Path

from s3_utils import download_seeds

import yaml
from dagster import Definitions, ScheduleDefinition, job, op, Field, Noneable

DBT_DIR = Path(__file__).parent / "dbt"
CONFIG_FILE = Path(__file__).parent / "pipeline_config.yml"

os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))


def _run_dbt(args: list[str]) -> None:
    """Execute a dbt command with a fallback to ``python -m dbt``.

    Raises a ``RuntimeError`` with captured output if the command fails or
    when dbt is not installed."""
    commands = [["dbt", *args], [sys.executable, "-m", "dbt", *args]]
    last_error: Exception | None = None
    for cmd in commands:
        try:
            result = subprocess.run(
                cmd,
                cwd=DBT_DIR,
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as exc:
            last_error = exc
            continue
        if result.returncode == 0:
            return
        last_error = RuntimeError(
            f"{' '.join(cmd)} failed with code {result.returncode}\n{result.stdout}\n{result.stderr}"
        )
    if last_error:
        raise last_error


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
    if "." not in fetcher:
        raise ValueError(
            f"Invalid fetcher '{fetcher}'. Expected dotted path like 'module.func'"
        )
    module_path, func_name = fetcher.rsplit(".", 1)
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)
    func()
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
    download_seeds(DBT_DIR / "seeds" / "external")
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


def load_config() -> dict:
    with open(CONFIG_FILE) as f:
        return yaml.safe_load(f) or {}


def active_models(cfg: dict) -> set[str]:
    models = cfg.get("models", [])
    return {m["name"] for m in models if m.get("active")}


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
