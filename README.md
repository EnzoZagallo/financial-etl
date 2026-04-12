# Financial Markets ETL Pipeline

A portfolio project demonstrating data engineering skills applied to financial market data. Extracts price and macroeconomic data from public APIs, computes technical indicators, and loads everything into a local PostgreSQL database.

---

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌──────────────┐
│   EXTRACT   │     │    TRANSFORM     │     │     LOAD     │
│             │     │                  │     │              │
│  yfinance   │────▶│  Clean nulls     │────▶│  Upsert to   │
│  FRED API   │     │  Calc returns    │     │  PostgreSQL  │
│             │     │  Technical ind.  │     │  (localhost) │
└─────────────┘     └──────────────────┘     └──────────────┘
       │                     │                       │
   extract.py          transform.py             load.py
                                                     │
                                              ┌──────┴───────┐
                                              │  PostgreSQL   │
                                              │  financial_etl│
                                              │  (local disk) │
                                              └──────────────┘
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Extraction | `yfinance`, `fredapi` |
| Transformation | `pandas`, `numpy` |
| Loading | `psycopg2` |
| Database | PostgreSQL (local) |
| Testing | `pytest`, `unittest.mock` |

---

## Data Sources

### Yahoo Finance (`yfinance`)
- Daily OHLCV prices for stocks, indices, ETFs, FX pairs, commodities
- No API key required
- Example tickers: `AAPL`, `MSFT`, `^GSPC`, `^VIX`, `GC=F`, `EURUSD=X`

### FRED — Federal Reserve Economic Data (`fredapi`)
- Macroeconomic indicators from the US Federal Reserve
- Free API key required: [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html)
- Series tracked: `GDP`, `CPIAUCSL`, `UNRATE`, `DFF`, `T10Y2Y`

---

## Database Schema

Four tables in the `financial_etl` PostgreSQL database:

| Table | Description |
|---|---|
| `assets` | Master list of tracked instruments |
| `daily_prices` | Historical OHLCV data per asset per day |
| `technical_indicators` | Computed indicators per asset per day |
| `macro_indicators` | FRED macro series values |

All writes use `INSERT ... ON CONFLICT DO UPDATE` — the pipeline is fully idempotent.

---

## Project Structure

```
financial-etl/
├── README.md
├── PROJECT_PLAN.md
├── CLAUDE.md                  # AI session context
├── requirements.txt
├── .env                       # DB credentials + FRED key (never commit)
├── .gitignore
│
├── config/
│   └── settings.py            # Loads .env, defines tickers/series/lookback
│
├── src/
│   ├── __init__.py
│   ├── extract.py             # yfinance + FRED API calls
│   ├── transform.py           # Data cleaning + indicator calculations
│   ├── load.py                # PostgreSQL upserts
│   └── pipeline.py            # Orchestrator + CLI
│
├── sql/
│   └── schema.sql             # CREATE TABLE statements + indexes
│
└── tests/
    ├── test_extract.py        # Mocked API tests
    └── test_transform.py      # Unit tests for cleaning + indicators
```

---

## Setup

### 1. PostgreSQL

Make sure PostgreSQL is running locally, then:

```bash
psql -U postgres -c "CREATE DATABASE financial_etl;"
psql -U postgres -d financial_etl -f sql/schema.sql
```

### 2. Python dependencies

```bash
pip install yfinance fredapi pandas psycopg2-binary python-dotenv pytest
```

### 3. Environment variables

Create a `.env` file in the project root:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=financial_etl
DB_USER=postgres
DB_PASSWORD=your_password

FRED_API_KEY=your_fred_api_key
```

Get a free FRED API key at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html).

---

## Running the Pipeline

```bash
# Full 2-year backfill for all tickers
python -m src.pipeline --mode backfill

# Last 5 days only (for daily scheduled runs)
python -m src.pipeline --mode daily

# Override tickers
python -m src.pipeline --mode backfill --tickers AAPL,MSFT

# Skip FRED macro extraction
python -m src.pipeline --mode backfill --skip-macro
```

---

## Running Tests

```bash
python -m pytest tests/ -v
```

29 tests — no real API calls (all mocked). Covers:
- Data cleaning edge cases (nulls, zeros, duplicates)
- Indicator math correctness (returns, SMA, EMA, RSI, volatility)
- API retry logic
- Empty DataFrame handling

---

## Indicators Computed

| Indicator | Formula |
|---|---|
| `daily_return` | `(close - prev_close) / prev_close` |
| `sma_20` | 20-day simple moving average of close |
| `sma_50` | 50-day simple moving average of close |
| `ema_12` | 12-day exponential moving average |
| `ema_26` | 26-day exponential moving average |
| `rsi_14` | 14-day Wilder's RSI |
| `volatility_30` | 30-day rolling std dev of daily returns |

---

## Useful Queries

```sql
-- Row counts across all tables
SELECT 'assets' AS tbl, COUNT(*) FROM assets
UNION ALL SELECT 'daily_prices', COUNT(*) FROM daily_prices
UNION ALL SELECT 'technical_indicators', COUNT(*) FROM technical_indicators
UNION ALL SELECT 'macro_indicators', COUNT(*) FROM macro_indicators;

-- Latest prices for all assets
SELECT a.ticker, p.date, p.close, p.volume
FROM daily_prices p
JOIN assets a ON a.asset_id = p.asset_id
WHERE p.date = (SELECT MAX(date) FROM daily_prices)
ORDER BY a.ticker;

-- RSI signals (overbought / oversold)
SELECT a.ticker, ROUND(t.rsi_14, 2) AS rsi,
    CASE WHEN t.rsi_14 > 70 THEN 'Overbought'
         WHEN t.rsi_14 < 30 THEN 'Oversold'
         ELSE 'Neutral' END AS signal
FROM technical_indicators t
JOIN assets a ON a.asset_id = t.asset_id
WHERE t.date = (SELECT MAX(date) FROM technical_indicators);

-- Latest macro values
SELECT indicator_code, indicator_name, date, value
FROM macro_indicators
WHERE (indicator_code, date) IN (
    SELECT indicator_code, MAX(date)
    FROM macro_indicators GROUP BY indicator_code
)
ORDER BY indicator_code;
```

---

## Sample Output

**Row counts after full backfill**
![Row counts](assets/screenshot_row_counts.png)

**All tracked assets across 3 asset classes**
![Assets](assets/screenshot_prices.png)

**RSI signals and moving averages — latest date across all tickers**
![Indicators](assets/screenshot_indicators.png)

**Latest macro indicator values from FRED**
![Macro](assets/screenshot_macro.png)

---

## Design Decisions

**Why upserts?** Plain `INSERT` fails on re-runs. `ON CONFLICT DO UPDATE` makes the pipeline idempotent — safe to run multiple times with the same result.

**Why `NUMERIC` instead of `FLOAT`?** Financial data requires precision. `FLOAT` introduces rounding errors. `NUMERIC(14,4)` stores exact decimal values.

**Why separate tables for indicators?** Raw prices and derived data are kept apart. If the RSI formula changes, just re-run the transform — raw data stays intact.

**Why mock the API in tests?** Tests run in ~1 second without network calls, never fail due to API downtime, and always return deterministic data.
