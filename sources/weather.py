from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import requests

DATA_PATH = (
    Path(__file__).resolve().parent.parent
    / "dbt"
    / "seeds"
    / "external"
    / "weather.csv"
)


def fetch() -> None:
    """Download recent weather data using the open-meteo API."""
    end = datetime.utcnow()
    start = end - timedelta(days=30)
    url = (
        "https://archive-api.open-meteo.com/v1/archive?latitude=41.8781"
        "&longitude=-87.6298"
        f"&start_date={start.date()}&end_date={end.date()}"
        "&hourly=temperature_2m"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    hourly = data.get("hourly", {})
    if not hourly:
        return
    df = pd.DataFrame({
        "timestamp": pd.to_datetime(hourly.get("time", [])),
        "temperature_c": hourly.get("temperature_2m", []),
    })
    if df.empty:
        return
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(DATA_PATH, index=False)


if __name__ == "__main__":
    fetch()
