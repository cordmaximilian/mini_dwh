from __future__ import annotations

import pandas as pd
import requests
from pathlib import Path
from utils import external_seed_path

SEASON_AVERAGES_PATH = external_seed_path("season_averages.csv")
PLAYERS_PATH = external_seed_path("players.csv")
TEAMS_PATH = external_seed_path("teams.csv")
GAMES_PATH = external_seed_path("games.csv")
GAME_STATS_PATH = external_seed_path("game_stats.csv")

API_BASE = "https://www.balldontlie.io/api/v1"


def _save_csv(path: Path, records: list[dict]) -> None:
    """Save records to ``path`` as CSV."""
    if not records:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame.from_records(records).to_csv(path, index=False)


def fetch_season_averages(season: int, max_player_id: int) -> None:
    """Fetch season averages for a range of players."""
    params = {
        "season": season,
        "player_ids[]": list(range(1, max_player_id + 1)),
    }
    try:
        resp = requests.get(f"{API_BASE}/season_averages", params=params, timeout=30)
        resp.raise_for_status()
    except Exception:
        return
    data = resp.json().get("data", [])
    for rec in data:
        rec["season"] = season
    _save_csv(SEASON_AVERAGES_PATH, data)


def _paginate(url: str, params: dict[str, int | str] | None = None, max_pages: int | None = None) -> list[dict]:
    records: list[dict] = []
    page = 1
    while True:
        page_params = {"page": page, "per_page": 100}
        if params:
            page_params.update(params)
        try:
            resp = requests.get(url, params=page_params, timeout=30)
            resp.raise_for_status()
        except Exception:
            break
        payload = resp.json()
        records.extend(payload.get("data", []))
        meta = payload.get("meta", {})
        if not meta.get("next_page"):
            break
        page += 1
        if max_pages and page > max_pages:
            break
    return records


def fetch_players(max_pages: int | None = None) -> None:
    records = _paginate(f"{API_BASE}/players", max_pages=max_pages)
    _save_csv(PLAYERS_PATH, records)


def fetch_teams() -> None:
    try:
        resp = requests.get(f"{API_BASE}/teams", timeout=30)
        resp.raise_for_status()
    except Exception:
        return
    _save_csv(TEAMS_PATH, resp.json().get("data", []))


def fetch_games(season: int, max_pages: int | None = None) -> None:
    records = _paginate(
        f"{API_BASE}/games",
        params={"seasons[]": season},
        max_pages=max_pages,
    )
    _save_csv(GAMES_PATH, records)


def fetch_game_stats(season: int, max_pages: int | None = None) -> None:
    records = _paginate(
        f"{API_BASE}/stats",
        params={"seasons[]": season},
        max_pages=max_pages,
    )
    _save_csv(GAME_STATS_PATH, records)


def fetch(season: int = 2022, max_player_id: int = 100, pages: int | None = 1) -> None:
    """Download basketball data from the balldontlie API."""
    fetch_season_averages(season, max_player_id)
    fetch_players(max_pages=pages)
    fetch_teams()
    fetch_games(season, max_pages=pages)
    fetch_game_stats(season, max_pages=pages)


if __name__ == "__main__":
    fetch()
