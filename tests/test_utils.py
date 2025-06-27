import types
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import yaml
except Exception:  # pragma: no cover - fallback stub
    import json
    yaml = ModuleType("yaml")
    def safe_load(stream):
        text = stream.read() if hasattr(stream, "read") else stream
        return json.loads(text or "{}")
    def safe_dump(obj, stream=None):
        text = json.dumps(obj, indent=2)
        if stream is None:
            return text
        stream.write(text)
        return text
    yaml.safe_load = safe_load
    yaml.safe_dump = safe_dump
    sys.modules["yaml"] = yaml

try:
    import duckdb
except Exception:  # pragma: no cover - fallback stub
    import sqlite3
    duckdb = ModuleType("duckdb")
    duckdb.connect = sqlite3.connect
    duckdb.DuckDBPyConnection = sqlite3.Connection
    sys.modules["duckdb"] = duckdb

try:
    import pandas as pd
except Exception:  # pragma: no cover - fallback stub
    pd = ModuleType("pandas")
    pd.DataFrame = types.SimpleNamespace
    pd.to_csv = lambda *a, **kw: None
    pd.to_datetime = lambda x: x
    sys.modules["pandas"] = pd

try:
    import dagster
except Exception:  # pragma: no cover - fallback stub
    dagster = ModuleType("dagster")
    class Definitions:
        def __init__(self, jobs=None, schedules=None):
            self.jobs = jobs
            self.schedules = schedules
    class ScheduleDefinition:
        def __init__(self, *args, **kwargs):
            pass
    def job(fn=None):
        def decorator(func):
            return func
        if fn is None:
            return decorator
        return decorator(fn)
    def op(*args, **kwargs):
        def decorator(func):
            def wrapper(*a, **kw):
                return func(*a, **kw)
            wrapper.__wrapped__ = func
            return wrapper
        if len(args) == 1 and callable(args[0]) and not kwargs:
            return decorator(args[0])
        return decorator
    class Field:
        def __init__(self, *args, **kwargs):
            pass
    class Noneable(list):
        pass
    dagster.Definitions = Definitions
    dagster.ScheduleDefinition = ScheduleDefinition
    dagster.job = job
    dagster.op = op
    dagster.Field = Field
    dagster.Noneable = Noneable
    sys.modules["dagster"] = dagster

import pytest
import utils
import run_pipeline
import cleanup_duckdb



def test_parse_cron():
    import importlib
    dagster_pipeline = importlib.import_module("dagster_pipeline")
    assert dagster_pipeline.parse_cron("hourly") == "0 * * * *"
    assert dagster_pipeline.parse_cron("daily") == "0 0 * * *"
    assert dagster_pipeline.parse_cron("weekly") == "0 0 * * 0"
    assert dagster_pipeline.parse_cron("every 2 hours") == "0 */2 * * *"
    assert dagster_pipeline.parse_cron("every 3 days") == "0 0 */3 * *"
    assert dagster_pipeline.parse_cron("14:30") == "30 14 * * *"




def test_invoke_fetcher_invalid():
    with pytest.raises(ValueError):
        utils.invoke_fetcher("invalid")


def test_invoke_fetcher_success(monkeypatch):
    module = types.ModuleType("dummy")
    called = {}
    def fetch():
        called["done"] = True
    module.fetch = fetch
    with monkeypatch.context() as m:
        m.setattr(utils.importlib, "import_module", lambda name: module)
        utils.invoke_fetcher("dummy.fetch")
    assert called.get("done")


def test_run_dbt_with_models():
    with patch("run_pipeline._run_dbt") as mock_run:
        run_pipeline.run_dbt(["m1", "m2"])
        mock_run.assert_any_call(["seed"])
        mock_run.assert_any_call(["run", "-s", "m1", "m2"])
        mock_run.assert_any_call(["test", "-s", "m1", "m2"])
        assert mock_run.call_count == 3


def test_run_dbt_without_models():
    with patch("run_pipeline._run_dbt") as mock_run:
        run_pipeline.run_dbt(None)
        mock_run.assert_any_call(["seed"])
        mock_run.assert_any_call(["run"])
        mock_run.assert_any_call(["test"])
        assert mock_run.call_count == 3


def test_fetch_calls_invoke_fetcher():
    with patch("run_pipeline.invoke_fetcher") as inv:
        run_pipeline.fetch("mod.func")
        inv.assert_called_with("mod.func")

def test_cleanup_removes_unused_tables(tmp_path, monkeypatch):
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE keepme (id INTEGER)")
    con.execute("CREATE TABLE removeme (id INTEGER)")
    con.close()
    monkeypatch.setattr(cleanup_duckdb, "DB_PATH", db_path)
    monkeypatch.setattr(cleanup_duckdb, "used_tables", lambda: {"keepme"})
    monkeypatch.setattr(cleanup_duckdb, "db_tables", lambda c: {"keepme", "removeme"})
    dropped = cleanup_duckdb.cleanup()
    assert set(dropped) == {"removeme"}


def test_basketball_fetch(monkeypatch, tmp_path):
    calls = {"params": [], "paths": []}

    # stub pandas before importing the module
    class DummyDF:
        def __init__(self, records):
            calls.setdefault("records", []).append(records)

        def to_csv(self, path, index=False):
            calls["paths"].append(str(path))

    dummy_pandas = ModuleType("pandas")
    dummy_pandas.DataFrame = types.SimpleNamespace(
        from_records=lambda rec: DummyDF(rec)
    )
    monkeypatch.setitem(sys.modules, "pandas", dummy_pandas)

    dummy_requests = ModuleType("requests")
    dummy_requests.get = lambda *a, **kw: None
    monkeypatch.setitem(sys.modules, "requests", dummy_requests)

    import importlib
    bb = importlib.import_module("sources.basketball")

    class DummyResp:
        def json(self):
            return {"data": [{"foo": "bar"}]}

        def raise_for_status(self):
            pass

    def fake_get(url, params=None, timeout=30):
        calls["params"].append(params or {})
        return DummyResp()

    monkeypatch.setattr(bb, "requests", types.SimpleNamespace(get=fake_get))
    monkeypatch.setattr(bb, "SEASON_AVERAGES_PATH", tmp_path / "avg.csv")
    monkeypatch.setattr(bb, "PLAYERS_PATH", tmp_path / "raw_players.csv")
    monkeypatch.setattr(bb, "TEAMS_PATH", tmp_path / "teams.csv")
    monkeypatch.setattr(bb, "GAMES_PATH", tmp_path / "games.csv")
    monkeypatch.setattr(bb, "GAME_STATS_PATH", tmp_path / "stats.csv")

    bb.fetch(max_player_id=2, pages=1)

    assert any(p.get("player_ids[]") == [1, 2] for p in calls["params"])
    assert any(str(p).endswith("avg.csv") for p in calls["paths"])
