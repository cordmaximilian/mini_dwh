from pathlib import Path

from . import EXTERNAL_DATA_DIR
import pandas as pd
import requests

DATA_PATH = EXTERNAL_DATA_DIR / "weather_forecast.csv"


def fetch() -> None:
    """Download a 7-day weather forecast using the open-meteo API."""
    url = (
        "https://api.open-meteo.com/v1/forecast?latitude=41.8781"
        "&longitude=-87.6298&hourly=temperature_2m&forecast_days=7"
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
