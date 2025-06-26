from prefect import task, flow
import os
import subprocess

from sources.commodities import fetch as fetch_commodities
from pathlib import Path

DBT_DIR = Path(__file__).parent / "mini_dwh_dbt"

# Ensure dbt uses the project-specific profile
os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))


@task
def fetch_data():
    """Download commodity prices and store them as a CSV."""
    fetch_commodities()


@task
def run_dbt_pipeline():
    """Execute the dbt commands for the full pipeline."""
    subprocess.run(["dbt", "seed"], check=True, cwd=DBT_DIR)
    subprocess.run(["dbt", "run"], check=True, cwd=DBT_DIR)
    subprocess.run(["dbt", "test"], check=True, cwd=DBT_DIR)


@flow
def full_pipeline() -> None:
    """Prefect flow that fetches data and runs dbt."""
    fetch_data()
    run_dbt_pipeline()


if __name__ == "__main__":
    full_pipeline()
