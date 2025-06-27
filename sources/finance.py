from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import os

import pandas as pd

try:
    import yfinance as yf
except Exception:  # pragma: no cover - optional dependency
    yf = None

try:
    from meteostat import Daily, Point
except Exception:  # pragma: no cover - optional dependency
    Daily = None
    Point = None

try:
    from newsapi import NewsApiClient
except Exception:  # pragma: no cover - optional dependency
    NewsApiClient = None

from utils import external_seed_path

STOCK_PRICES_PATH = external_seed_path("stock_prices.csv")
COMMODITY_PRICES_PATH = external_seed_path("commodity_prices.csv")
WEATHER_PATH = external_seed_path("weather.csv")
NEWS_PATH = external_seed_path("news.csv")


START_DATE = date.today() - timedelta(days=5 * 365)
END_DATE = date.today()


# Example lists - adjust as needed
STOCK_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM",
    "JNJ", "V", "PG", "UNH", "HD", "MA", "BAC", "PFE", "DIS", "INTC",
    "VZ", "KO",
]

COMMODITY_TICKERS = [
    "GC=F",  # Gold
    "CL=F",  # Crude Oil
    "SI=F",  # Silver
]


API_KEY_ENV = "NEWSAPI_KEY"


def _save_csv(path: Path, df: pd.DataFrame) -> None:
    if df.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def fetch_stock_prices(tickers: list[str] | None = None) -> None:
    if yf is None:
        return
    if tickers is None:
        tickers = STOCK_TICKERS
    data = yf.download(
        tickers=tickers,
        start=START_DATE.strftime("%Y-%m-%d"),
        end=END_DATE.strftime("%Y-%m-%d"),
        group_by="ticker",
        threads=True,
    )
    records: list[pd.DataFrame] = []
    if isinstance(data, pd.DataFrame) and "Adj Close" in data.columns:
        data = {"PRICE": data}
    for ticker in tickers:
        if ticker in data:
            df = data[ticker].copy()
            df.reset_index(inplace=True)
            df.insert(0, "ticker", ticker)
            records.append(df)
    if records:
        df_out = pd.concat(records)
        _save_csv(STOCK_PRICES_PATH, df_out)


def fetch_commodity_prices(tickers: list[str] | None = None) -> None:
    if yf is None:
        return
    if tickers is None:
        tickers = COMMODITY_TICKERS
    data = yf.download(
        tickers=tickers,
        start=START_DATE.strftime("%Y-%m-%d"),
        end=END_DATE.strftime("%Y-%m-%d"),
        group_by="ticker",
        threads=True,
    )
    records: list[pd.DataFrame] = []
    for ticker in tickers:
        if ticker in data:
            df = data[ticker].copy()
            df.reset_index(inplace=True)
            df.insert(0, "ticker", ticker)
            records.append(df)
    if records:
        df_out = pd.concat(records)
        _save_csv(COMMODITY_PRICES_PATH, df_out)


def fetch_weather(lat: float = 40.7128, lon: float = -74.0060) -> None:
    if Daily is None or Point is None:
        return
    location = Point(lat, lon)
    data = Daily(location, START_DATE, END_DATE)
    df = data.fetch().reset_index()
    _save_csv(WEATHER_PATH, df)


def fetch_news() -> None:
    if NewsApiClient is None:
        return
    api_key = os.getenv(API_KEY_ENV)
    if not api_key:
        return
    client = NewsApiClient(api_key=api_key)
    all_articles = []
    from_date = START_DATE
    while from_date < END_DATE:
        to_date = from_date + timedelta(days=30)
        resp = client.get_everything(
            q="stock market",
            from_param=from_date.strftime("%Y-%m-%d"),
            to=to_date.strftime("%Y-%m-%d"),
            language="en",
            sort_by="relevancy",
            page_size=100,
        )
        articles = resp.get("articles", [])
        for art in articles:
            art["date"] = art.get("publishedAt", "")[:10]
        all_articles.extend(articles)
        from_date = to_date
    if all_articles:
        df = pd.DataFrame(all_articles)
        _save_csv(NEWS_PATH, df)


def fetch() -> None:
    """Fetch financial data, weather and news."""
    fetch_stock_prices()
    fetch_commodity_prices()
    fetch_weather()
    fetch_news()


if __name__ == "__main__":
    fetch()
