from pathlib import Path

import pandas as pd
import requests

DATA_PATH = (
    Path(__file__).resolve().parent.parent
    / "dbt"
    / "seeds"
    / "external"
    / "world_bank_gdp.csv"
)


def fetch() -> None:
    """Download recent GDP data from the World Bank."""
    countries = ["USA", "CAN", "MEX"]
    frames = []
    for country in countries:
        url = (
            f"https://api.worldbank.org/v2/country/{country}/"
            "indicator/NY.GDP.MKTP.CD?format=json"
        )
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        values = data[1] if len(data) > 1 else []
        for entry in values[:10]:
            year = entry.get("date")
            value = entry.get("value")
            if value is None:
                continue
            frames.append({
                "country": country,
                "year": int(year),
                "gdp_usd": float(value),
            })
    if not frames:
        return
    df = pd.DataFrame(frames)
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(DATA_PATH, index=False)


if __name__ == "__main__":
    fetch()
