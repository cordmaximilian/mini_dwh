from datetime import datetime, timezone
from utils import external_seed_path

import pandas as pd
import requests

DATA_PATH = external_seed_path("exchange_rates.csv")


def fetch() -> None:
    """Download latest USD exchange rates."""
    url = "https://api.exchangerate.host/latest?base=USD"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    rates = data.get("rates", {})
    timestamp = datetime.now(timezone.utc)
    currencies = ["EUR", "GBP", "JPY"]
    rows = [
        {"timestamp": timestamp, "currency": c, "rate": rates.get(c)}
        for c in currencies
        if c in rates
    ]
    if not rows:
        return
    df = pd.DataFrame(rows)
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(DATA_PATH, index=False)


if __name__ == "__main__":
    fetch()
