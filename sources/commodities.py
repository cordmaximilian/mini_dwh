from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import yfinance as yf


DATA_PATH = (
    Path(__file__).resolve().parent.parent
    / "dbt"
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
        # Crude oil futures
        "oil": "CL=F",
        # TODO: Replace with a valid fertilizer price ticker
        "fertilizer": "FTR.F",
    }
    end = datetime.utcnow()
    start = end - timedelta(days=720)
    frames = []
    for name, ticker in tickers.items():
        df = yf.download(ticker, start=start, end=end, interval="1h")
        if df.empty:
            continue
        # Ensure the index is named so reset_index produces a ``timestamp`` column
        df.index.name = "timestamp"
        df = df.reset_index()
        # yfinance may return ``Close`` or ``Adj Close`` depending on options
        price_col = "Close" if "Close" in df.columns else "Adj Close"
        df.rename(columns={price_col: "price"}, inplace=True)
        df["commodity"] = name
        frames.append(df[["timestamp", "price", "commodity"]])
    if not frames:
        return
    result = pd.concat(frames)
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(DATA_PATH, index=False)


if __name__ == "__main__":
    fetch()
