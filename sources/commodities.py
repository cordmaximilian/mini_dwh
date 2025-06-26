from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf


DATA_PATH = (
    Path(__file__).resolve().parent.parent
    / "mini_dwh_dbt"
    / "seeds"
    / "external"
    / "commodity_prices.csv"
)


def fetch() -> None:
    """Download commodity prices for roughly the last two years."""
    tickers = {
        "wheat": "ZW=F",
        "corn": "ZC=F",
        "soybeans": "ZS=F",
    }
    end = datetime.utcnow()
    start = end - timedelta(days=720)
    frames = []
    for name, ticker in tickers.items():
        df = yf.download(ticker, start=start, end=end, interval="1h")
        if df.empty:
            continue
        df = df.reset_index()
        df.rename(columns={df.columns[0]: "timestamp", "Close": "price"}, inplace=True)
        df["commodity"] = name
        frames.append(df[["timestamp", "price", "commodity"]])
    if not frames:
        return
    result = pd.concat(frames)
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(DATA_PATH, index=False)


if __name__ == "__main__":
    fetch()
