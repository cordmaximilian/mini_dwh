from __future__ import annotations

from pathlib import Path
import importlib
import subprocess
import sys

DBT_DIR = Path(__file__).parent / "dbt"


def external_seed_path(filename: str) -> Path:
    """Return the path to an external seed file in the dbt directory."""
    return DBT_DIR / "seeds" / "external" / filename


def _run_dbt(args: list[str]) -> None:
    """Execute a dbt command with a fallback to ``python -m dbt.cli.main``."""
    commands = [["dbt", *args], [sys.executable, "-m", "dbt.cli.main", *args]]
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


def invoke_fetcher(path: str) -> None:
    """Import and run the ``fetch`` function referenced by ``path``."""
    if "." not in path:
        raise ValueError("Expected '<module>.<function>'")
    module_name, func_name = path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    fetcher = getattr(module, func_name, None)
    if not callable(fetcher):
        raise ValueError(f"Invalid fetcher: {path}")
    fetcher()

