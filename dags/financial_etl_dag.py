"""
Airflow DAG — Financial Markets ETL Pipeline

Schedule: weekdays at 07:00 CET (05:00 UTC)
Tasks:    extract_prices → transform_prices → load_prices
          extract_macro  → load_macro
"""

import json
import logging
from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from config.settings import TICKER_UNIVERSE, FRED_SERIES
from src.extract import extract_prices, extract_macro
from src.transform import clean_prices, calculate_indicators
from src.load import get_connection, upsert_asset, upsert_prices, upsert_indicators, upsert_macro

logger = logging.getLogger(__name__)

# ── Asset metadata (same as pipeline.py) ───────────────────────────────────────

ASSET_METADATA = {
    "AAPL":     {"name": "Apple Inc.",           "asset_type": "stock",     "sector": "Technology",  "currency": "USD"},
    "MSFT":     {"name": "Microsoft Corp.",      "asset_type": "stock",     "sector": "Technology",  "currency": "USD"},
    "^GSPC":    {"name": "S&P 500",              "asset_type": "index",     "sector": None,          "currency": "USD"},
    "^VIX":     {"name": "CBOE Volatility Index","asset_type": "index",     "sector": None,          "currency": "USD"},
    "GC=F":     {"name": "Gold Futures",         "asset_type": "commodity", "sector": None,          "currency": "USD"},
    "EURUSD=X": {"name": "EUR/USD",              "asset_type": "fx",        "sector": None,          "currency": "USD"},
}


# ── Date range for daily mode ──────────────────────────────────────────────────

def _daily_range() -> tuple[str, str]:
    """Return (start, end) as ISO strings for the last 5 days."""
    today = date.today()
    start = today - timedelta(days=5)
    return start.isoformat(), today.isoformat()


# ── Task functions ─────────────────────────────────────────────────────────────

def task_extract_prices(**context):
    """Extract raw OHLCV data for all tickers, push to XCom."""
    start_str, end_str = _daily_range()
    start = date.fromisoformat(start_str)
    end = date.fromisoformat(end_str)

    results = {}
    for ticker in TICKER_UNIVERSE:
        logger.info(f"Extracting: {ticker}")
        df = extract_prices(ticker, start, end)
        if not df.empty:
            results[ticker] = df.to_json(orient="split", date_format="iso")
        else:
            logger.warning(f"No data for {ticker}")

    context["ti"].xcom_push(key="raw_prices", value=results)
    logger.info(f"Extracted {len(results)}/{len(TICKER_UNIVERSE)} tickers")


def task_transform_prices(**context):
    """Clean prices and compute indicators, push to XCom."""
    import pandas as pd

    raw_data = context["ti"].xcom_pull(task_ids="extract_prices", key="raw_prices")

    clean_data = {}
    indicator_data = {}

    for ticker, raw_json in raw_data.items():
        logger.info(f"Transforming: {ticker}")
        df = pd.read_json(raw_json, orient="split")
        df["date"] = pd.to_datetime(df["date"]).dt.date

        cleaned = clean_prices(df)
        if cleaned.empty:
            logger.warning(f"No valid rows after cleaning: {ticker}")
            continue

        indicators = calculate_indicators(cleaned)

        clean_data[ticker] = cleaned.to_json(orient="split", date_format="iso")
        indicator_data[ticker] = indicators.to_json(orient="split", date_format="iso")

    context["ti"].xcom_push(key="clean_prices", value=clean_data)
    context["ti"].xcom_push(key="indicators", value=indicator_data)
    logger.info(f"Transformed {len(clean_data)} tickers")


def task_load_prices(**context):
    """Load cleaned prices and indicators into PostgreSQL."""
    import pandas as pd

    clean_data = context["ti"].xcom_pull(task_ids="transform_prices", key="clean_prices")
    indicator_data = context["ti"].xcom_pull(task_ids="transform_prices", key="indicators")

    conn = get_connection()
    try:
        for ticker in clean_data:
            logger.info(f"Loading: {ticker}")

            meta = ASSET_METADATA.get(ticker, {
                "name": ticker, "asset_type": "stock",
                "sector": None, "currency": "USD",
            })
            asset_id = upsert_asset(conn, ticker, **meta)

            price_df = pd.read_json(clean_data[ticker], orient="split")
            price_df["date"] = pd.to_datetime(price_df["date"]).dt.date
            upsert_prices(conn, asset_id, price_df)

            ind_df = pd.read_json(indicator_data[ticker], orient="split")
            ind_df["date"] = pd.to_datetime(ind_df["date"]).dt.date
            upsert_indicators(conn, asset_id, ind_df)

        logger.info(f"Loaded {len(clean_data)} tickers to PostgreSQL")
    finally:
        conn.close()


def task_extract_macro(**context):
    """Extract all FRED macro series, push to XCom."""
    start_str, end_str = _daily_range()
    start = date.fromisoformat(start_str)
    end = date.fromisoformat(end_str)

    results = {}
    for series_id in FRED_SERIES:
        logger.info(f"Extracting FRED: {series_id}")
        df = extract_macro(series_id, start, end)
        if not df.empty:
            results[series_id] = df.to_json(orient="split", date_format="iso")
        else:
            logger.warning(f"No data for {series_id}")

    context["ti"].xcom_push(key="raw_macro", value=results)
    logger.info(f"Extracted {len(results)}/{len(FRED_SERIES)} FRED series")


def task_load_macro(**context):
    """Load FRED macro data into PostgreSQL."""
    import pandas as pd

    raw_data = context["ti"].xcom_pull(task_ids="extract_macro", key="raw_macro")

    conn = get_connection()
    try:
        for series_id, raw_json in raw_data.items():
            logger.info(f"Loading FRED: {series_id}")
            df = pd.read_json(raw_json, orient="split")
            df["date"] = pd.to_datetime(df["date"]).dt.date
            indicator_name = FRED_SERIES.get(series_id, series_id)
            upsert_macro(conn, series_id, indicator_name, df)

        logger.info(f"Loaded {len(raw_data)} FRED series to PostgreSQL")
    finally:
        conn.close()


# ── DAG definition ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["your_email@example.com"],  # Replace with your email for alerts
}

with DAG(
    dag_id="financial_etl_daily",
    default_args=default_args,
    description="Daily ETL pipeline for financial market data",
    schedule_interval="0 5 * * 1-5",  # 05:00 UTC = 07:00 CET, weekdays only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["financial", "etl"],
) as dag:

    extract_prices_task = PythonOperator(
        task_id="extract_prices",
        python_callable=task_extract_prices,
    )

    transform_prices_task = PythonOperator(
        task_id="transform_prices",
        python_callable=task_transform_prices,
    )

    load_prices_task = PythonOperator(
        task_id="load_prices",
        python_callable=task_load_prices,
    )

    extract_macro_task = PythonOperator(
        task_id="extract_macro",
        python_callable=task_extract_macro,
    )

    load_macro_task = PythonOperator(
        task_id="load_macro",
        python_callable=task_load_macro,
    )

    # Task dependencies
    # Prices: extract → transform → load
    extract_prices_task >> transform_prices_task >> load_prices_task

    # Macro: extract → load (no transform needed)
    extract_macro_task >> load_macro_task
