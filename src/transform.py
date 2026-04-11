import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def clean_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and clean a raw prices DataFrame.

    - Drops rows with null close price
    - Removes duplicate (date) rows, keeping the last
    - Ensures correct column types
    - Removes rows where close <= 0
    """
    if df.empty:
        return df

    initial_len = len(df)

    df = df.dropna(subset=["close"])
    df = df[df["close"] > 0]
    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)

    df["open"]      = pd.to_numeric(df["open"],      errors="coerce")
    df["high"]      = pd.to_numeric(df["high"],      errors="coerce")
    df["low"]       = pd.to_numeric(df["low"],       errors="coerce")
    df["close"]     = pd.to_numeric(df["close"],     errors="coerce")
    df["adj_close"] = pd.to_numeric(df["adj_close"], errors="coerce")
    df["volume"]    = pd.to_numeric(df["volume"],    errors="coerce").astype("Int64")

    dropped = initial_len - len(df)
    if dropped:
        logger.warning(f"  clean_prices: dropped {dropped} invalid rows")

    logger.info(f"  clean_prices: {len(df)} rows after cleaning")
    return df


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute technical indicators from a cleaned prices DataFrame.

    Input must have columns: date, close
    Returns a DataFrame with columns:
        date, daily_return, sma_20, sma_50, ema_12, ema_26, rsi_14, volatility_30
    """
    if df.empty:
        return pd.DataFrame()

    df = df.sort_values("date").reset_index(drop=True)
    out = pd.DataFrame()
    out["date"] = df["date"]

    close = df["close"]

    # Daily return: (close - prev_close) / prev_close
    out["daily_return"] = close.pct_change()

    # Simple moving averages
    out["sma_20"] = close.rolling(window=20, min_periods=20).mean()
    out["sma_50"] = close.rolling(window=50, min_periods=50).mean()

    # Exponential moving averages
    out["ema_12"] = close.ewm(span=12, adjust=False).mean()
    out["ema_26"] = close.ewm(span=26, adjust=False).mean()

    # RSI-14
    out["rsi_14"] = _rsi(close, period=14)

    # 30-day rolling volatility (annualised std dev of daily returns)
    out["volatility_30"] = (
        out["daily_return"].rolling(window=30, min_periods=30).std()
    )

    out = out.reset_index(drop=True)
    logger.info(f"  calculate_indicators: {len(out)} rows computed")
    return out


# ── helpers ───────────────────────────────────────────────────────────────────

def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Wilder's smoothed RSI."""
    delta = close.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs  = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi
