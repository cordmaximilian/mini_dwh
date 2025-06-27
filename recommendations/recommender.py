from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "warehouse.duckdb"
OUTPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "recommendations.csv"
QUERY = (
    "SELECT date, stock_ticker, stock_close FROM gold.daily_market_data ORDER BY date"
)


def generate(
    output_path: Path = OUTPUT_PATH,
    window: int = 5,
    query: str | None = None,
) -> pd.DataFrame:
    """Create simple trading recommendations based on moving averages."""
    con = duckdb.connect(str(DB_PATH))
    cursor = con.execute(query or QUERY)
    try:
        df = cursor.fetch_df()
    except AttributeError:  # pragma: no cover - sqlite fallback
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
    con.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values(["stock_ticker", "date"], inplace=True)
    df["ma"] = (
        df.groupby("stock_ticker")["stock_close"].transform(lambda s: s.rolling(window).mean())
    )
    df["recommendation"] = df.apply(
        lambda r: "BUY" if r["stock_close"] < r["ma"] else "SELL", axis=1
    )
    result = df[["date", "stock_ticker", "recommendation"]].dropna()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)
    return result
