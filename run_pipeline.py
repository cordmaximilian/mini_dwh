"""Run a single pipeline execution without Dagster."""

import argparse
import importlib
import subprocess
import sys
from pathlib import Path

import yaml
from s3_utils import download_seeds

DBT_DIR = Path(__file__).parent / "dbt"
CONFIG_FILE = Path(__file__).parent / "pipeline_config.yml"


def _run_dbt(args: list[str]) -> None:
    """Execute a dbt command with a fallback to ``python -m dbt``.

    Raises ``RuntimeError`` if the command fails or dbt is not installed."""
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
    download_seeds(DBT_DIR / "seeds" / "external")
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

    models = args.models if args.models is not None else active_models(cfg)

    fetch(fetcher)
    run_dbt(models)


if __name__ == "__main__":
    main()
