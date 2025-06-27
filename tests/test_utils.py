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
import register_model
import run_pipeline
import cleanup_duckdb



def test_parse_cron(monkeypatch):
    import importlib
    monkeypatch.setattr(utils, "load_config", lambda: {})
    dagster_pipeline = importlib.import_module("dagster_pipeline")
    assert dagster_pipeline.parse_cron("hourly") == "0 * * * *"
    assert dagster_pipeline.parse_cron("daily") == "0 0 * * *"
    assert dagster_pipeline.parse_cron("weekly") == "0 0 * * 0"
    assert dagster_pipeline.parse_cron("every 2 hours") == "0 */2 * * *"
    assert dagster_pipeline.parse_cron("every 3 days") == "0 0 */3 * *"
    assert dagster_pipeline.parse_cron("14:30") == "30 14 * * *"


def test_active_models():
    cfg = {"models": [{"name": "a", "active": True}, {"name": "b", "active": False}]}
    assert utils.active_models(cfg) == {"a"}


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


def test_set_model_state(tmp_path, monkeypatch):
    cfg_file = tmp_path / "cfg.yml"
    cfg_file.write_text(yaml.safe_dump({"models": [{"name": "a", "active": True}]}))
    monkeypatch.setattr(register_model, "CONFIG_FILE", cfg_file)
    register_model.set_model_state("a", False)
    data = yaml.safe_load(cfg_file.read_text())
    assert data["models"][0]["active"] is False
    register_model.set_model_state("b", True)
    data = yaml.safe_load(cfg_file.read_text())
    names = {m["name"] for m in data["models"]}
    assert names == {"a", "b"}


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
