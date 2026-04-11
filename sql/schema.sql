-- ============================================================
-- Financial ETL Pipeline — Database Schema
-- Database: financial_etl (PostgreSQL, local)
-- Run: psql -U postgres -d financial_etl -f sql/schema.sql
-- ============================================================

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
    UNIQUE(asset_id, date)
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

-- ============================================================
-- Indexes for query performance
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_prices_asset_date
    ON daily_prices(asset_id, date);

CREATE INDEX IF NOT EXISTS idx_indicators_asset_date
    ON technical_indicators(asset_id, date);

CREATE INDEX IF NOT EXISTS idx_macro_date
    ON macro_indicators(indicator_code, date);
