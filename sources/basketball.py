import csv
import requests
from utils import external_seed_path

DATA_PATH = external_seed_path("season_averages.csv")

API_URL = "https://www.balldontlie.io/api/v1/season_averages"


def fetch(season: int = 2022, max_players: int = 100) -> None:
    """Download NBA season averages using the balldontlie API."""
    params = {
        "season": season,
        "player_ids[]": list(range(1, max_players + 1)),
    }
    try:
        resp = requests.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
    except Exception:
        return
    data = resp.json().get("data", [])
    records = []
    for record in data:
        record["season"] = season
        records.append(record)
    if not records:
        return
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sorted(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)


if __name__ == "__main__":
    fetch()
