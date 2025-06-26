"""Run a single pipeline execution without Dagster."""

import argparse
import importlib
import subprocess
from pathlib import Path

import yaml

DBT_DIR = Path(__file__).parent / "dbt"
CONFIG_FILE = Path(__file__).parent / "pipeline_config.yml"


def load_config() -> dict:
    with open(CONFIG_FILE) as f:
        return yaml.safe_load(f) or {}


def active_models(cfg: dict) -> list[str]:
    return [m.get("name") for m in cfg.get("models", []) if m.get("active")]


def fetch(fetcher: str) -> None:
    if "." not in fetcher:
        raise ValueError(f"Invalid fetcher '{fetcher}'. Expected 'module.func'")
    module_path, func_name = fetcher.rsplit(".", 1)
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)
    func()


def run_dbt(models: list[str] | None) -> None:
    subprocess.run(["dbt", "seed"], check=True, cwd=DBT_DIR)
    if models:
        subprocess.run(["dbt", "run", "-s", *models], check=True, cwd=DBT_DIR)
        subprocess.run(["dbt", "test", "-s", *models], check=True, cwd=DBT_DIR)
    else:
        subprocess.run(["dbt", "run"], check=True, cwd=DBT_DIR)
        subprocess.run(["dbt", "test"], check=True, cwd=DBT_DIR)


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

    models = args.models if args.models is not None else active_models(cfg)

    fetch(fetcher)
    run_dbt(models)


if __name__ == "__main__":
    main()
