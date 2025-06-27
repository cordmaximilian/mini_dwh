"""Run a single pipeline execution without Dagster."""
import argparse
from utils import _run_dbt, load_config, active_models, invoke_fetcher



def fetch(fetcher: str) -> None:
    invoke_fetcher(fetcher)


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
    parser.add_argument("--fetcher", help="Dotted path to fetch function")
    parser.add_argument("--models", nargs="*", help="dbt models to run")
    args = parser.parse_args()

    cfg = load_config()

    fetcher = args.fetcher
    if not fetcher:
        if cfg.get("sources"):
            fetcher = cfg["sources"][0]["fetcher"]
        else:
            parser.error("No fetcher specified and no sources in config")

    models = args.models if args.models is not None else list(active_models(cfg))

    fetch(fetcher)
    run_dbt(models)


if __name__ == "__main__":
    main()
