from utils import _run_dbt, invoke_fetcher


def run_dbt(models: list[str] | None) -> None:
    """Execute dbt for the given models."""
    _run_dbt(["seed"])
    if models:
        _run_dbt(["run", "-s", *models])
        _run_dbt(["test", "-s", *models])
    else:
        _run_dbt(["run"])
        _run_dbt(["test"])


def fetch(fetcher: str) -> None:
    """Call the configured fetcher."""
    invoke_fetcher(fetcher)
