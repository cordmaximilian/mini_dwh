import os
import subprocess
import schedule
import time

import yaml
import importlib
import logging


from sources.commodities import fetch as fetch_commodities



from pathlib import Path

DBT_DIR = Path(__file__).parent / "mini_dwh_dbt"
CONFIG_FILE = Path(__file__).parent / "pipeline_config.yml"

# Ensure dbt uses the project-specific profile rather than the default
os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))

# Ensure the DuckDB directory exists before running dbt
def _ensure_duckdb_dir() -> None:
    """Create the parent directory for the DuckDB file if needed."""
    path = os.environ.get("DUCKDB_PATH")
    if path:
        dir_path = Path(path).expanduser().parent
    else:
        dir_path = DBT_DIR / "data"
    dir_path.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def run_dbt_pipeline(models: list[str]) -> None:
    """Run dbt commands for the selected models."""
    logging.info("Running dbt models: %s", ", ".join(models))
    _ensure_duckdb_dir()
    subprocess.run(["dbt", "seed"], check=True, cwd=DBT_DIR)
    if models:
        subprocess.run(["dbt", "run", "-s", *models], check=True, cwd=DBT_DIR)
        subprocess.run(["dbt", "test", "-s", *models], check=True, cwd=DBT_DIR)
    else:
        subprocess.run(["dbt", "run"], check=True, cwd=DBT_DIR)
        subprocess.run(["dbt", "test"], check=True, cwd=DBT_DIR)


def load_config() -> dict:
    """Load pipeline configuration from ``pipeline_config.yml``."""
    with open(CONFIG_FILE) as f:
        return yaml.safe_load(f) or {}


def active_models(cfg: dict) -> set[str]:
    """Return the names of active dbt models from the config."""
    models = cfg.get("models", [])
    return {m["name"] for m in models if m.get("active")}


def schedule_source(source: dict, active: set[str]) -> None:
    """Schedule fetching and dbt execution for a single source."""
    name = source.get("name", "unknown")
    fetcher_path = source["fetcher"]
    source_models = [m for m in source.get("models", []) if m in active]
    sched = source.get("schedule", "hourly")

    module_path, func_name = fetcher_path.rsplit(".", 1)
    fetch_module = importlib.import_module(module_path)
    fetch_func = getattr(fetch_module, func_name)

    def run_source() -> None:
        logging.info("Running source '%s'", name)
        fetch_func()
        run_dbt_pipeline(source_models)

    job = schedule.every()
    s = str(sched).lower()
    if s == "hourly":
        job = job.hour
    elif s == "daily":
        job = job.day
    elif s == "weekly":
        job = job.week
    elif s.startswith("every"):
        parts = s.split()
        if len(parts) >= 3:
            interval = int(parts[1])
            unit = parts[2].rstrip("s")
            job = getattr(schedule.every(interval), unit)
        else:
            raise ValueError(f"Invalid schedule format: {sched}")
    else:
        # Assume HH:MM format for daily execution
        job = job.day.at(sched)

    job.do(run_source)
    # Run once at startup
    run_source()



def run_full_pipeline():
    """Fetch commodity data and run the dbt pipeline."""
    cfg = load_config()
    fetch_commodities()
    run_dbt_pipeline(sorted(active_models(cfg)))


def main() -> None:
    cfg = load_config()
    active = active_models(cfg)
    for source in cfg.get("sources", []):
        schedule_source(source, active)

    logging.info("Starting orchestration loop. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
