from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import importlib

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
    """Import and execute a fetcher given by dotted path."""
    if "." not in path:
        raise ValueError(f"Invalid fetcher '{path}'. Expected 'module.func'")
    module_path, func_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)
    func()
