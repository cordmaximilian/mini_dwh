from __future__ import annotations

import importlib
import os
import subprocess
from pathlib import Path

from prefect import flow, task

DBT_DIR = Path(__file__).parent / "mini_dwh_dbt"

# Ensure dbt uses the project-specific profile
os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))


@task
def fetch_data(fetcher: str) -> None:
    """Import and execute the configured fetch function."""
    module_path, func_name = fetcher.rsplit(".", 1)
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)
    func()


@task
def run_dbt_pipeline(models: list[str] | None = None) -> None:
    """Execute dbt for the selected models."""
    subprocess.run(["dbt", "seed"], check=True, cwd=DBT_DIR)
    if models:
        subprocess.run(["dbt", "run", "-s", *models], check=True, cwd=DBT_DIR)
        subprocess.run(["dbt", "test", "-s", *models], check=True, cwd=DBT_DIR)
    else:
        subprocess.run(["dbt", "run"], check=True, cwd=DBT_DIR)
        subprocess.run(["dbt", "test"], check=True, cwd=DBT_DIR)


@flow
def pipeline(fetcher: str, models: list[str] | None = None) -> None:
    """Prefect flow that fetches data and runs dbt."""
    fetch_data(fetcher)
    run_dbt_pipeline(models)


if __name__ == "__main__":
    pipeline("sources.commodities.fetch", [])

