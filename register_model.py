"""Toggle active state of dbt models in ``pipeline_config.yml``."""

from __future__ import annotations

import argparse
import yaml
from pathlib import Path


CONFIG_FILE = Path(__file__).parent / "pipeline_config.yml"


def load_config() -> dict:
    with open(CONFIG_FILE) as f:
        return yaml.safe_load(f) or {}


def save_config(cfg: dict) -> None:
    with open(CONFIG_FILE, "w") as f:
        yaml.safe_dump(cfg, f)


def set_model_state(model_name: str, active: bool) -> None:
    cfg = load_config()
    models: list[dict] = cfg.get("models", [])
    for model in models:
        if model.get("name") == model_name:
            model["active"] = active
            break
    else:
        # Add new model entry if it does not exist
        models.append({"name": model_name, "active": active})
    cfg["models"] = models
    save_config(cfg)


def main() -> None:
    parser = argparse.ArgumentParser(description="Toggle model active state")
    parser.add_argument("model", help="Name of the model")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--activate", action="store_true", help="Activate model")
    group.add_argument("--deactivate", action="store_true", help="Deactivate model")
    args = parser.parse_args()

    set_model_state(args.model, active=args.activate)


if __name__ == "__main__":
    main()

