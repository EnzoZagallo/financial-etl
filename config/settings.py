import os
from dotenv import load_dotenv

load_dotenv()

# ── Database ──────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "financial_etl")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ── FRED API ──────────────────────────────────────────────────
FRED_API_KEY = os.getenv("FRED_API_KEY", "")

# ── Tickers to track ──────────────────────────────────────────
TICKER_UNIVERSE = [
    "AAPL",     # Apple — stock
    "MSFT",     # Microsoft — stock
    "^GSPC",    # S&P 500 — index
    "^VIX",     # VIX volatility index
    "GC=F",     # Gold futures — commodity
    "EURUSD=X", # EUR/USD — fx
]

# ── FRED macro series ─────────────────────────────────────────
FRED_SERIES = {
    "GDP":      "Gross Domestic Product",
    "CPIAUCSL": "Consumer Price Index (All Urban Consumers)",
    "UNRATE":   "Unemployment Rate",
    "DFF":      "Federal Funds Effective Rate",
    "T10Y2Y":   "10Y-2Y Treasury Spread",
}

# ── Pipeline defaults ─────────────────────────────────────────
LOOKBACK_DAYS = 730  # 2 years of historical data
