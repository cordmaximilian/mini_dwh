import sys
from types import ModuleType
import pytest

try:
    import duckdb
except Exception:  # pragma: no cover - fallback stub
    import sqlite3
    duckdb = ModuleType("duckdb")
    duckdb.connect = sqlite3.connect
    duckdb.DuckDBPyConnection = sqlite3.Connection
    sys.modules["duckdb"] = duckdb

pd = pytest.importorskip("pandas")

import recommendations.recommender as rec

def test_generate_recommendations(tmp_path, monkeypatch):
    db = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db))
    con.execute(
        "CREATE TABLE daily_market_data (date DATE, stock_ticker TEXT, stock_close DOUBLE)"
    )
    for i in range(6):
        con.execute(
            "INSERT INTO daily_market_data VALUES (?, 'AAPL', ?)",
            (f'2023-01-0{i+1}', 100 + i),
        )
    con.close()

    out = tmp_path / "out.csv"
    monkeypatch.setattr(rec, "DB_PATH", db)
    df = rec.generate(output_path=out, window=3, query="SELECT date, stock_ticker, stock_close FROM daily_market_data ORDER BY date")
    assert out.exists()
    assert not df.empty
    assert {"date", "stock_ticker", "recommendation"}.issubset(df.columns)
