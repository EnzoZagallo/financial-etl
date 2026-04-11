import logging
import time
from datetime import date

import pandas as pd
import yfinance as yf
from fredapi import Fred

from config.settings import FRED_API_KEY

logger = logging.getLogger(__name__)


def extract_prices(ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
    """
    Pull daily OHLCV data for a single ticker from Yahoo Finance.

    Returns a DataFrame with columns:
        date, open, high, low, close, adj_close, volume
    Returns an empty DataFrame on failure.
    """
    logger.info(f"Extracting prices: {ticker} ({start_date} → {end_date})")

    for attempt in range(1, 4):
        try:
            raw = yf.download(
                ticker,
                start=start_date,
                end=end_date,
                auto_adjust=False,
                progress=False,
            )

            if raw.empty:
                logger.warning(f"No data returned for {ticker}")
                return pd.DataFrame()

            # yfinance returns MultiIndex columns when downloading a single ticker
            # with auto_adjust=False — flatten them
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)

            df = raw.rename(columns={
                "Open":      "open",
                "High":      "high",
                "Low":       "low",
                "Close":     "close",
                "Adj Close": "adj_close",
                "Volume":    "volume",
            })

            df = df[["open", "high", "low", "close", "adj_close", "volume"]].copy()
            df.index.name = "date"
            df = df.reset_index()
            df["date"] = pd.to_datetime(df["date"]).dt.date

            logger.info(f"  {ticker}: {len(df)} rows extracted")
            return df

        except Exception as e:
            logger.error(f"  Attempt {attempt}/3 failed for {ticker}: {e}")
            if attempt < 3:
                time.sleep(2 ** attempt)  # exponential backoff: 2s, 4s

    logger.error(f"All retries exhausted for {ticker}")
    return pd.DataFrame()


def extract_macro(series_id: str, start_date: date, end_date: date) -> pd.DataFrame:
    """
    Pull a single FRED macro series.

    Returns a DataFrame with columns:
        date, value
    Returns an empty DataFrame on failure.
    """
    if not FRED_API_KEY:
        raise ValueError(
            "FRED_API_KEY is not set. Add it to your .env file.\n"
            "Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html"
        )

    logger.info(f"Extracting FRED series: {series_id} ({start_date} → {end_date})")

    for attempt in range(1, 4):
        try:
            fred = Fred(api_key=FRED_API_KEY)
            series = fred.get_series(
                series_id,
                observation_start=start_date,
                observation_end=end_date,
            )

            if series.empty:
                logger.warning(f"No data returned for FRED series {series_id}")
                return pd.DataFrame()

            df = series.reset_index()
            df.columns = ["date", "value"]
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df = df.dropna(subset=["value"])

            logger.info(f"  {series_id}: {len(df)} rows extracted")
            return df

        except Exception as e:
            logger.error(f"  Attempt {attempt}/3 failed for {series_id}: {e}")
            if attempt < 3:
                time.sleep(2 ** attempt)

    logger.error(f"All retries exhausted for {series_id}")
    return pd.DataFrame()
