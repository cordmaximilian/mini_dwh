import pandas as pd
import requests
from utils import external_seed_path

DATA_PATH = external_seed_path("season_averages.csv")

API_URL = "https://www.balldontlie.io/api/v1/season_averages"


def fetch(season: int = 2022, max_players: int = 100) -> None:
    """Download NBA season averages using the balldontlie API."""
    records: list[dict] = []
    for player_id in range(1, max_players + 1):
        url = f"{API_URL}?season={season}&player_ids[]={player_id}"
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
        except Exception:
            continue
        data = resp.json().get("data", [])
        if not data:
            continue
        record = data[0]
        record["player_id"] = player_id
        record["season"] = season
        records.append(record)
    if not records:
        return
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame.from_records(records)
    df.to_csv(DATA_PATH, index=False)


if __name__ == "__main__":
    fetch()
