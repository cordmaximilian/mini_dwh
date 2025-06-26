import os
import subprocess
import schedule
import time

from fetch_commodity_prices import fetch_commodity_prices


from pathlib import Path

DBT_DIR = Path(__file__).parent / "mini_dwh_dbt"

# Ensure dbt uses the project-specific profile rather than the default
os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))


def run_dbt_pipeline():
    """Run dbt commands for the full pipeline."""
    subprocess.run(["dbt", "seed"], check=True, cwd=DBT_DIR)
    subprocess.run(["dbt", "run"], check=True, cwd=DBT_DIR)
    subprocess.run(["dbt", "test"], check=True, cwd=DBT_DIR)


def run_full_pipeline():
    """Fetch commodity data and run the dbt pipeline."""
    fetch_commodity_prices()
    run_dbt_pipeline()


def main():
    schedule.every().hour.do(run_full_pipeline)
    print("Starting orchestration loop. Press Ctrl+C to stop.")
    run_full_pipeline()
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
