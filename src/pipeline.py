"""
pipeline.py — ETL orchestrator

Usage:
    python -m src.pipeline --mode backfill
    python -m src.pipeline --mode daily
    python -m src.pipeline --mode backfill --tickers AAPL,MSFT
    python -m src.pipeline --mode daily --tickers ^GSPC
"""

import argparse
import logging
import sys
from datetime import date, timedelta

from config.settings import (
    TICKER_UNIVERSE,
    FRED_SERIES,
    LOOKBACK_DAYS,
)
from src.extract import extract_prices, extract_macro
from src.transform import clean_prices, calculate_indicators
from src.load import (
    get_connection,
    upsert_asset,
    upsert_prices,
    upsert_indicators,
    upsert_macro,
)

# ── Logging setup ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ── Asset metadata ─────────────────────────────────────────────────────────────
# Extend this dict when you add new tickers to TICKER_UNIVERSE.

ASSET_METADATA = {
    "AAPL":     {"name": "Apple Inc.",          "asset_type": "stock",     "sector": "Technology",  "currency": "USD"},
    "MSFT":     {"name": "Microsoft Corp.",      "asset_type": "stock",     "sector": "Technology",  "currency": "USD"},
    "^GSPC":    {"name": "S&P 500",              "asset_type": "index",     "sector": None,          "currency": "USD"},
    "^VIX":     {"name": "CBOE Volatility Index","asset_type": "index",     "sector": None,          "currency": "USD"},
    "GC=F":     {"name": "Gold Futures",         "asset_type": "commodity", "sector": None,          "currency": "USD"},
    "EURUSD=X": {"name": "EUR/USD",              "asset_type": "fx",        "sector": None,          "currency": "USD"},
}


# ── Date helpers ───────────────────────────────────────────────────────────────

def _date_range(mode: str) -> tuple[date, date]:
    today = date.today()
    if mode == "backfill":
        return today - timedelta(days=LOOKBACK_DAYS), today
    else:  # daily
        return today - timedelta(days=5), today


# ── Price pipeline ─────────────────────────────────────────────────────────────

def run_prices(conn, tickers: list[str], start: date, end: date) -> dict:
    """Extract → Transform → Load prices and indicators for each ticker."""
    results = {"success": [], "failed": []}

    for ticker in tickers:
        logger.info(f"── {ticker} ──────────────────────────────")
        try:
            # Extract
            raw = extract_prices(ticker, start, end)
            if raw.empty:
                logger.warning(f"  No data for {ticker}, skipping")
                results["failed"].append(ticker)
                continue

            # Transform
            clean = clean_prices(raw)
            if clean.empty:
                logger.warning(f"  No valid rows after cleaning for {ticker}, skipping")
                results["failed"].append(ticker)
                continue

            indicators = calculate_indicators(clean)

            # Load — asset first to get asset_id
            meta = ASSET_METADATA.get(ticker, {
                "name": ticker, "asset_type": "stock",
                "sector": None, "currency": "USD",
            })
            asset_id = upsert_asset(conn, ticker, **meta)
            upsert_prices(conn, asset_id, clean)
            upsert_indicators(conn, asset_id, indicators)

            results["success"].append(ticker)

        except Exception as e:
            logger.error(f"  FAILED {ticker}: {e}", exc_info=True)
            results["failed"].append(ticker)

    return results


# ── Macro pipeline ─────────────────────────────────────────────────────────────

def run_macro(conn, start: date, end: date) -> dict:
    """Extract → Load macro indicators from FRED."""
    results = {"success": [], "failed": []}

    for series_id, indicator_name in FRED_SERIES.items():
        logger.info(f"── FRED: {series_id} ──────────────────────────────")
        try:
            df = extract_macro(series_id, start, end)
            if df.empty:
                logger.warning(f"  No data for {series_id}, skipping")
                results["failed"].append(series_id)
                continue

            upsert_macro(conn, series_id, indicator_name, df)
            results["success"].append(series_id)

        except Exception as e:
            logger.error(f"  FAILED {series_id}: {e}", exc_info=True)
            results["failed"].append(series_id)

    return results


# ── Entrypoint ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Financial ETL Pipeline")
    parser.add_argument(
        "--mode",
        choices=["backfill", "daily"],
        required=True,
        help="backfill: full history | daily: last 5 days",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Comma-separated tickers to override TICKER_UNIVERSE (e.g. AAPL,MSFT)",
    )
    parser.add_argument(
        "--skip-macro",
        action="store_true",
        help="Skip FRED macro extraction (useful if FRED_API_KEY is not yet set)",
    )
    args = parser.parse_args()

    tickers = (
        [t.strip() for t in args.tickers.split(",")]
        if args.tickers
        else TICKER_UNIVERSE
    )

    start, end = _date_range(args.mode)
    logger.info(f"Pipeline starting  mode={args.mode}  range={start} → {end}")
    logger.info(f"Tickers: {tickers}")

    conn = get_connection()
    try:
        price_results = run_prices(conn, tickers, start, end)

        macro_results = {"success": [], "failed": []}
        if not args.skip_macro:
            macro_results = run_macro(conn, start, end)
        else:
            logger.info("Skipping macro extraction (--skip-macro flag set)")

    finally:
        conn.close()

    # ── Summary ────────────────────────────────────────────────────────────────
    logger.info("─" * 55)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"  Prices   — success: {price_results['success']}")
    if price_results["failed"]:
        logger.warning(f"  Prices   — failed:  {price_results['failed']}")
    logger.info(f"  Macro    — success: {macro_results['success']}")
    if macro_results["failed"]:
        logger.warning(f"  Macro    — failed:  {macro_results['failed']}")

    if price_results["failed"] or macro_results["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
