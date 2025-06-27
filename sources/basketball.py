import pandas as pd
import requests
from utils import external_seed_path

DATA_PATH = external_seed_path("season_averages.csv")

API_URL = "https://www.balldontlie.io/api/v1/season_averages"


def fetch(season: int = 2022, max_player_id: int = 100, batch_size: int = 25) -> None:
    """Download NBA season averages using the balldontlie API."""
    records: list[dict] = []
    for start in range(1, max_player_id + 1, batch_size):
        ids = range(start, min(start + batch_size, max_player_id + 1))
        params = [("season", season)] + [("player_ids[]", pid) for pid in ids]
        try:
            resp = requests.get(API_URL, params=params, timeout=30)
            resp.raise_for_status()
        except Exception:
            continue
        data = resp.json().get("data", [])
        if not data:
            continue
        records.extend(data)
    if not records:
        return
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame.from_records(records)
    df.to_csv(DATA_PATH, index=False)


if __name__ == "__main__":
    fetch()
