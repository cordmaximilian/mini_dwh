"""Run a single pipeline execution without Dagster."""
import argparse
from utils import _run_dbt
from sources.basketball import fetch



MODELS = [
    "player_stats",
    "player_efficiency",
]

def fetch_data() -> None:
    fetch()


def run_dbt(models: list[str] | None) -> None:
    _run_dbt(["seed"])
    if models:
        _run_dbt(["run", "-s", *models])
        _run_dbt(["test", "-s", *models])
    else:
        _run_dbt(["run"])
        _run_dbt(["test"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute pipeline once")
    parser.add_argument("--models", nargs="*", help="dbt models to run")
    args = parser.parse_args()

    models = args.models if args.models is not None else MODELS

    fetch_data()
    run_dbt(models)


if __name__ == "__main__":
    main()
