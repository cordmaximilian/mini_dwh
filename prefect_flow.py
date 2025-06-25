from prefect import task, flow
import subprocess

from fetch_commodity_prices import fetch_commodity_prices


@task
def fetch_data():
    """Download commodity prices and store them as a CSV."""
    fetch_commodity_prices()


@task
def run_dbt_pipeline():
    """Execute the dbt commands for the full pipeline."""
    subprocess.run(["dbt", "seed"], check=True)
    subprocess.run(["dbt", "run"], check=True)
    subprocess.run(["dbt", "test"], check=True)


@flow
def full_pipeline() -> None:
    """Prefect flow that fetches data and runs dbt."""
    fetch_data()
    run_dbt_pipeline()


if __name__ == "__main__":
    full_pipeline()
