import os
import requests

ALPACA_KEY_ENV = "ALPACA_API_KEY"
ALPACA_SECRET_ENV = "ALPACA_SECRET_KEY"
ALPACA_URL_ENV = "ALPACA_BASE_URL"
DEFAULT_BASE_URL = "https://paper-api.alpaca.markets"


def get_session(api_key: str | None = None, secret_key: str | None = None,
                base_url: str | None = None) -> requests.Session:
    """Return a configured ``requests.Session`` for the Alpaca API."""
    api_key = api_key or os.getenv(ALPACA_KEY_ENV)
    secret_key = secret_key or os.getenv(ALPACA_SECRET_ENV)
    base_url = base_url or os.getenv(ALPACA_URL_ENV, DEFAULT_BASE_URL)
    if not api_key or not secret_key:
        raise ValueError("Missing Alpaca API credentials")
    session = requests.Session()
    session.headers.update({
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key,
    })
    session.base_url = base_url  # type: ignore[attr-defined]
    return session
