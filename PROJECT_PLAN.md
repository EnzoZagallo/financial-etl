# Financial Markets ETL Pipeline — Project Plan & Context

## Overview

An ETL (Extract, Transform, Load) pipeline for financial market data, built as a portfolio project demonstrating data engineering and data science skills with a focus on economics and financial analysis.

**Tech Stack:**
- **Python** — extraction and transformation (pandas, yfinance, fredapi)
- **PostgreSQL** — locally hosted relational database
- **psycopg2 / SQLAlchemy** — Python ↔ PostgreSQL connectivity

**Goal:** Pull financial market and macroeconomic data from public APIs, clean and enrich it with calculated indicators, and store it in a well-modeled PostgreSQL database for analysis.

---

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌──────────────┐
│   EXTRACT   │     │    TRANSFORM     │     │     LOAD     │
│             │     │                  │     │              │
│  yfinance   │────▶│  Clean nulls     │────▶│  Upsert to   │
│  FRED API   │     │  Calc returns    │     │  PostgreSQL  │
│             │     │  Technical ind.  │     │  (localhost)  │
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

## Data Sources

### 1. Yahoo Finance (via `yfinance` library)
- **What:** Daily OHLCV prices for stocks, indices, ETFs, FX pairs
- **Auth:** None required (free, no API key)
- **Install:** `pip install yfinance`
- **Example tickers:** `AAPL`, `^GSPC` (S&P 500), `EURUSD=X`, `GC=F` (Gold futures)

### 2. FRED — Federal Reserve Economic Data (via `fredapi`)
- **What:** Macroeconomic indicators (GDP, CPI, unemployment, fed funds rate)
- **Auth:** Free API key from https://fred.stlouisfed.org/docs/api/api_key.html
- **Install:** `pip install fredapi`
- **Key series IDs:**
  - `GDP` — Gross Domestic Product
  - `CPIAUCSL` — Consumer Price Index
  - `UNRATE` — Unemployment Rate
  - `DFF` — Federal Funds Effective Rate
  - `T10Y2Y` — 10Y-2Y Treasury Spread (recession indicator)

---

## Database Design

### PostgreSQL Setup (Local)

PostgreSQL stores all data on your local disk. No cloud services needed.

**Initial setup commands (run once):**

```sql
-- Connect to PostgreSQL via pgAdmin or psql terminal
CREATE DATABASE financial_etl;

-- Then connect to the new database:
-- psql -d financial_etl
```

### Schema: 4 Core Tables

```sql
-- 1. ASSETS — Master table of tracked instruments
CREATE TABLE IF NOT EXISTS assets (
    asset_id    SERIAL PRIMARY KEY,
    ticker      VARCHAR(20) UNIQUE NOT NULL,
    name        VARCHAR(100),
    asset_type  VARCHAR(20) NOT NULL,  -- 'stock', 'index', 'etf', 'fx', 'commodity'
    sector      VARCHAR(50),
    currency    VARCHAR(10) DEFAULT 'USD',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. DAILY_PRICES — Historical OHLCV data
CREATE TABLE IF NOT EXISTS daily_prices (
    price_id    SERIAL PRIMARY KEY,
    asset_id    INTEGER REFERENCES assets(asset_id),
    date        DATE NOT NULL,
    open        NUMERIC(14,4),
    high        NUMERIC(14,4),
    low         NUMERIC(14,4),
    close       NUMERIC(14,4),
    adj_close   NUMERIC(14,4),
    volume      BIGINT,
    UNIQUE(asset_id, date)  -- Prevents duplicates on re-runs
);

-- 3. TECHNICAL_INDICATORS — Calculated metrics
CREATE TABLE IF NOT EXISTS technical_indicators (
    indicator_id  SERIAL PRIMARY KEY,
    asset_id      INTEGER REFERENCES assets(asset_id),
    date          DATE NOT NULL,
    daily_return  NUMERIC(10,6),
    sma_20        NUMERIC(14,4),   -- 20-day Simple Moving Average
    sma_50        NUMERIC(14,4),   -- 50-day Simple Moving Average
    ema_12        NUMERIC(14,4),   -- 12-day Exponential Moving Average
    ema_26        NUMERIC(14,4),   -- 26-day Exponential Moving Average
    rsi_14        NUMERIC(8,4),    -- 14-day Relative Strength Index
    volatility_30 NUMERIC(10,6),   -- 30-day rolling std dev of returns
    UNIQUE(asset_id, date)
);

-- 4. MACRO_INDICATORS — Economic data from FRED
CREATE TABLE IF NOT EXISTS macro_indicators (
    macro_id        SERIAL PRIMARY KEY,
    indicator_code  VARCHAR(20) NOT NULL,  -- FRED series ID
    indicator_name  VARCHAR(100),
    date            DATE NOT NULL,
    value           NUMERIC(16,4),
    UNIQUE(indicator_code, date)
);

-- Indexes for query performance
CREATE INDEX idx_prices_asset_date ON daily_prices(asset_id, date);
CREATE INDEX idx_indicators_asset_date ON technical_indicators(asset_id, date);
CREATE INDEX idx_macro_date ON macro_indicators(indicator_code, date);
```

---

## Project File Structure

```
financial-etl/
├── README.md                  # Project overview, setup instructions, screenshots
├── PROJECT_PLAN.md            # This file — full context and planning
├── requirements.txt           # Python dependencies
├── .env                       # DB credentials (DO NOT commit — add to .gitignore)
├── .gitignore
│
├── config/
│   └── settings.py            # Load .env, define ticker universe, FRED series
│
├── src/
│   ├── __init__.py
│   ├── extract.py             # Pull data from yfinance and FRED
│   ├── transform.py           # Clean data + calculate indicators
│   ├── load.py                # Upsert data into PostgreSQL
│   └── pipeline.py            # Orchestrator: ties E → T → L with logging
│
├── sql/
│   └── schema.sql             # CREATE TABLE statements (copy from above)
│
├── tests/
│   ├── test_extract.py
│   └── test_transform.py      # Unit tests for indicator calculations
│
└── notebooks/
    └── exploration.ipynb       # EDA, charts, correlation analysis
```

---

## Development Steps (ordered)

Use these steps when working with Claude Code. Each step is a self-contained task.

### Step 1 — Database Schema
- Create `sql/schema.sql` with the table definitions above
- Run it against your local PostgreSQL to create the tables
- Verify with `\dt` in psql or check pgAdmin

### Step 2 — Configuration
- Create `.env` with `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- Create `config/settings.py` to load env vars and define:
  - `TICKER_UNIVERSE`: list of tickers to track (start with 5-10)
  - `FRED_SERIES`: list of macro indicator codes
  - `LOOKBACK_DAYS`: default historical window (e.g., 730 for 2 years)

### Step 3 — Extractor (`src/extract.py`)
- `extract_prices(ticker, start_date, end_date)` → returns raw DataFrame from yfinance
- `extract_macro(series_id, start_date, end_date)` → returns FRED data as DataFrame
- Include error handling for API failures and rate limits
- Add logging for each extraction

### Step 4 — Transformer (`src/transform.py`)
- `clean_prices(df)` → handle nulls, validate data types, remove duplicates
- `calculate_indicators(df)` → compute:
  - Daily returns: `(close - prev_close) / prev_close`
  - SMA 20 & 50: `close.rolling(window).mean()`
  - EMA 12 & 26: `close.ewm(span).mean()`
  - RSI 14: standard RSI formula
  - 30-day volatility: `returns.rolling(30).std()`
- All transform functions should be pure (no side effects) for easy testing

### Step 5 — Loader (`src/load.py`)
- `upsert_asset(ticker, name, asset_type, ...)` → INSERT ... ON CONFLICT DO UPDATE
- `upsert_prices(asset_id, df)` → bulk upsert daily prices
- `upsert_indicators(asset_id, df)` → bulk upsert calculated indicators
- `upsert_macro(series_id, df)` → bulk upsert macro data
- **Critical:** Use upserts (not plain inserts) so re-running the pipeline is safe

### Step 6 — Pipeline Orchestrator (`src/pipeline.py`)
- Wire together: extract → transform → load
- Add CLI arguments:
  - `--mode backfill` (load full history) vs `--mode daily` (last 5 days)
  - `--tickers AAPL,MSFT` (override default universe)
- Add logging with timestamps and record counts
- Wrap in try/except with meaningful error messages

### Step 7 — Tests
- Test `calculate_indicators()` with known input/output
- Test `clean_prices()` edge cases (missing data, zero volume days)
- Use `pytest`

### Step 8 — README & Documentation
- What the project does
- Architecture diagram (can reuse the ASCII one above)
- Setup instructions (PostgreSQL, Python env, .env file)
- How to run: `python -m src.pipeline --mode backfill`
- Sample output / screenshots from pgAdmin or notebooks

---

## Key Python Dependencies

```
yfinance>=0.2.0
fredapi>=0.5.0
pandas>=2.0.0
psycopg2-binary>=2.9.0
sqlalchemy>=2.0.0
python-dotenv>=1.0.0
pytest>=7.0.0
```

---

## Design Decisions & Best Practices

### Why Upserts?
Plain `INSERT` fails on re-runs because of unique constraints. Using `INSERT ... ON CONFLICT (asset_id, date) DO UPDATE` makes the pipeline **idempotent** — you can run it 10 times and get the same result. This is essential for production ETL.

### Why Separate Tables for Indicators?
Keeping raw prices separate from calculated indicators follows the principle of separating raw data from derived data. If you change your RSI calculation, you just re-run the transform — raw data stays intact.

### Why NUMERIC Instead of FLOAT?
Financial data requires precision. `FLOAT` introduces rounding errors. `NUMERIC(14,4)` stores exact values — important when dealing with prices and returns.

### .env for Credentials
Never hardcode database passwords. The `.env` file keeps secrets local, and `.gitignore` ensures they never reach GitHub.

---

## Future Extensions (nice-to-have)

- **Scheduler:** Use `cron` (Linux/Mac) or Task Scheduler (Windows) to run daily
- **Airflow/Prefect:** Swap `pipeline.py` for a proper DAG orchestrator
- **Dashboard:** Build a Streamlit or Dash app to visualize the data
- **More data:** Add earnings data, news sentiment, options data
- **Docker:** Containerize PostgreSQL + pipeline for reproducibility
- **Data quality checks:** Add assertions (e.g., no future dates, close > 0)

---

## Quick Reference: PostgreSQL Commands

```bash
# Start PostgreSQL (varies by OS)
# Windows: services.msc → PostgreSQL → Start
# Mac:     brew services start postgresql
# Linux:   sudo systemctl start postgresql

# Connect via terminal
psql -U postgres -d financial_etl

# Useful psql commands
\dt              -- list all tables
\d daily_prices  -- describe table structure
\conninfo        -- show connection info

# Check row counts
SELECT 'assets' AS tbl, COUNT(*) FROM assets
UNION ALL
SELECT 'daily_prices', COUNT(*) FROM daily_prices
UNION ALL
SELECT 'technical_indicators', COUNT(*) FROM technical_indicators
UNION ALL
SELECT 'macro_indicators', COUNT(*) FROM macro_indicators;
```

---

*Last updated: April 2026*
*Use this document as context when starting new Claude Code sessions for this project.*
