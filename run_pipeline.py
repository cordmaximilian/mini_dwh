"""Run a single pipeline execution without Dagster."""
import argparse
import importlib

from utils import load_config, active_models, run_dbt_steps



def fetch(fetcher: str) -> None:
    if "." not in fetcher:
        raise ValueError(f"Invalid fetcher '{fetcher}'. Expected 'module.func'")
    module_path, func_name = fetcher.rsplit(".", 1)
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)
    func()


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
    run_dbt_steps(models)


if __name__ == "__main__":
    main()
