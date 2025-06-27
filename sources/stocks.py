from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

DATA_PATH = (
    Path(__file__).resolve().parent.parent
    / "dbt"
    / "seeds"
    / "external"
    / "stock_prices.csv"
)


def fetch() -> None:
    """Download recent stock prices using yfinance."""
    tickers = {
        "AAPL": "AAPL",
        "GOOG": "GOOG",
        "MSFT": "MSFT",
    }
    end = datetime.utcnow()
    start = end - timedelta(days=30)
    frames = []
    for name, ticker in tickers.items():
        df = yf.download(ticker, start=start, end=end, interval="1d")
        if df.empty:
            continue
        df.index.name = "timestamp"
        df = df.reset_index()
        price_col = "Close" if "Close" in df.columns else "Adj Close"
        df.rename(columns={price_col: "price"}, inplace=True)
        df["ticker"] = name
        frames.append(df[["timestamp", "price", "ticker"]])
    if not frames:
        return
    result = pd.concat(frames)
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(DATA_PATH, index=False)


if __name__ == "__main__":
    fetch()
