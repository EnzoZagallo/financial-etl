import logging
from datetime import date

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PgConnection

from config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)


# ── Connection ─────────────────────────────────────────────────────────────────

def get_connection() -> PgConnection:
    """Open and return a psycopg2 connection to financial_etl."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


# ── Assets ─────────────────────────────────────────────────────────────────────

def upsert_asset(
    conn: PgConnection,
    ticker: str,
    name: str,
    asset_type: str,
    sector: str = None,
    currency: str = "USD",
) -> int:
    """
    Insert or update an asset row. Returns the asset_id.

    On conflict (ticker already exists), updates name/sector/currency.
    """
    sql = """
        INSERT INTO assets (ticker, name, asset_type, sector, currency)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (ticker) DO UPDATE
            SET name     = EXCLUDED.name,
                sector   = EXCLUDED.sector,
                currency = EXCLUDED.currency
        RETURNING asset_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ticker, name, asset_type, sector, currency))
        asset_id = cur.fetchone()[0]
    conn.commit()
    logger.info(f"  upsert_asset: {ticker} → asset_id={asset_id}")
    return asset_id


def get_asset_id(conn: PgConnection, ticker: str) -> int | None:
    """Return asset_id for a ticker, or None if not found."""
    with conn.cursor() as cur:
        cur.execute("SELECT asset_id FROM assets WHERE ticker = %s", (ticker,))
        row = cur.fetchone()
    return row[0] if row else None


# ── Daily Prices ───────────────────────────────────────────────────────────────

def upsert_prices(conn: PgConnection, asset_id: int, df: pd.DataFrame) -> int:
    """
    Bulk upsert daily price rows for a given asset_id.

    df must have columns: date, open, high, low, close, adj_close, volume
    Returns the number of rows processed.
    """
    if df.empty:
        logger.warning(f"  upsert_prices: empty DataFrame for asset_id={asset_id}, skipping")
        return 0

    sql = """
        INSERT INTO daily_prices
            (asset_id, date, open, high, low, close, adj_close, volume)
        VALUES %s
        ON CONFLICT (asset_id, date) DO UPDATE
            SET open      = EXCLUDED.open,
                high      = EXCLUDED.high,
                low       = EXCLUDED.low,
                close     = EXCLUDED.close,
                adj_close = EXCLUDED.adj_close,
                volume    = EXCLUDED.volume
    """
    rows = [
        (
            asset_id,
            row.date,
            _nan_to_none(row.open),
            _nan_to_none(row.high),
            _nan_to_none(row.low),
            _nan_to_none(row.close),
            _nan_to_none(row.adj_close),
            _nan_to_none(row.volume),
        )
        for row in df.itertuples(index=False)
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    conn.commit()

    logger.info(f"  upsert_prices: {len(rows)} rows → asset_id={asset_id}")
    return len(rows)


# ── Technical Indicators ───────────────────────────────────────────────────────

def upsert_indicators(conn: PgConnection, asset_id: int, df: pd.DataFrame) -> int:
    """
    Bulk upsert technical indicator rows for a given asset_id.

    df must have columns:
        date, daily_return, sma_20, sma_50, ema_12, ema_26, rsi_14, volatility_30
    Returns the number of rows processed.
    """
    if df.empty:
        logger.warning(f"  upsert_indicators: empty DataFrame for asset_id={asset_id}, skipping")
        return 0

    sql = """
        INSERT INTO technical_indicators
            (asset_id, date, daily_return, sma_20, sma_50, ema_12, ema_26, rsi_14, volatility_30)
        VALUES %s
        ON CONFLICT (asset_id, date) DO UPDATE
            SET daily_return  = EXCLUDED.daily_return,
                sma_20        = EXCLUDED.sma_20,
                sma_50        = EXCLUDED.sma_50,
                ema_12        = EXCLUDED.ema_12,
                ema_26        = EXCLUDED.ema_26,
                rsi_14        = EXCLUDED.rsi_14,
                volatility_30 = EXCLUDED.volatility_30
    """
    rows = [
        (
            asset_id,
            row.date,
            _nan_to_none(row.daily_return),
            _nan_to_none(row.sma_20),
            _nan_to_none(row.sma_50),
            _nan_to_none(row.ema_12),
            _nan_to_none(row.ema_26),
            _nan_to_none(row.rsi_14),
            _nan_to_none(row.volatility_30),
        )
        for row in df.itertuples(index=False)
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    conn.commit()

    logger.info(f"  upsert_indicators: {len(rows)} rows → asset_id={asset_id}")
    return len(rows)


# ── Macro Indicators ───────────────────────────────────────────────────────────

def upsert_macro(
    conn: PgConnection,
    series_id: str,
    indicator_name: str,
    df: pd.DataFrame,
) -> int:
    """
    Bulk upsert macro indicator rows.

    df must have columns: date, value
    Returns the number of rows processed.
    """
    if df.empty:
        logger.warning(f"  upsert_macro: empty DataFrame for {series_id}, skipping")
        return 0

    sql = """
        INSERT INTO macro_indicators
            (indicator_code, indicator_name, date, value)
        VALUES %s
        ON CONFLICT (indicator_code, date) DO UPDATE
            SET indicator_name = EXCLUDED.indicator_name,
                value          = EXCLUDED.value
    """
    rows = [
        (series_id, indicator_name, row.date, _nan_to_none(row.value))
        for row in df.itertuples(index=False)
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    conn.commit()

    logger.info(f"  upsert_macro: {len(rows)} rows → {series_id}")
    return len(rows)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _nan_to_none(value):
    """Convert NaN/NaT to None and numpy scalars to native Python types."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    # psycopg2 can't handle numpy types — convert to native Python
    if hasattr(value, "item"):
        return value.item()
    return value
