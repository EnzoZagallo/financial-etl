"""
Tests for src/transform.py

All tests use synthetic DataFrames — no API calls, no database.
"""

import math
from datetime import date

import pandas as pd
import pytest

from src.transform import clean_prices, calculate_indicators


# ── Fixtures ───────────────────────────────────────────────────────────────────

def make_prices(closes: list[float], start: str = "2024-01-01") -> pd.DataFrame:
    """Build a minimal prices DataFrame from a list of close values."""
    dates = pd.date_range(start=start, periods=len(closes), freq="B").date
    return pd.DataFrame({
        "date":      dates,
        "open":      closes,
        "high":      [c * 1.01 for c in closes],
        "low":       [c * 0.99 for c in closes],
        "close":     closes,
        "adj_close": closes,
        "volume":    [1_000_000] * len(closes),
    })


# ── clean_prices ───────────────────────────────────────────────────────────────

class TestCleanPrices:

    def test_returns_same_rows_when_data_is_clean(self):
        df = make_prices([100.0, 101.0, 102.0])
        result = clean_prices(df)
        assert len(result) == 3

    def test_drops_null_close(self):
        df = make_prices([100.0, 101.0, 102.0])
        df.loc[1, "close"] = None
        result = clean_prices(df)
        assert len(result) == 2

    def test_drops_zero_close(self):
        df = make_prices([100.0, 0.0, 102.0])
        result = clean_prices(df)
        assert len(result) == 2

    def test_drops_negative_close(self):
        df = make_prices([100.0, -5.0, 102.0])
        result = clean_prices(df)
        assert len(result) == 2

    def test_removes_duplicate_dates(self):
        df = make_prices([100.0, 101.0, 102.0])
        df = pd.concat([df, df.iloc[[1]]], ignore_index=True)  # duplicate row 1
        result = clean_prices(df)
        assert len(result) == 3

    def test_empty_input_returns_empty(self):
        result = clean_prices(pd.DataFrame())
        assert result.empty

    def test_output_sorted_by_date(self):
        df = make_prices([100.0, 101.0, 102.0])
        df = df.iloc[::-1].reset_index(drop=True)  # reverse order
        result = clean_prices(df)
        assert list(result["date"]) == sorted(result["date"])

    def test_volume_cast_to_int(self):
        df = make_prices([100.0, 101.0])
        df["volume"] = [1500000.0, 2000000.0]
        result = clean_prices(df)
        assert pd.api.types.is_integer_dtype(result["volume"])


# ── calculate_indicators ───────────────────────────────────────────────────────

class TestCalculateIndicators:

    def test_returns_same_number_of_rows(self):
        df = make_prices([float(100 + i) for i in range(60)])
        result = calculate_indicators(df)
        assert len(result) == 60

    def test_daily_return_correct(self):
        df = make_prices([100.0, 110.0, 99.0])
        result = calculate_indicators(df)
        # row 0: NaN (no previous), row 1: +10%, row 2: -10%
        assert math.isnan(result.loc[0, "daily_return"])
        assert abs(result.loc[1, "daily_return"] - 0.10) < 1e-6
        assert abs(result.loc[2, "daily_return"] - (-0.10)) < 1e-6

    def test_sma_20_nan_before_20_rows(self):
        df = make_prices([float(100 + i) for i in range(25)])
        result = calculate_indicators(df)
        # First 19 rows should be NaN (min_periods=20)
        assert result.loc[:18, "sma_20"].isna().all()
        assert not math.isnan(result.loc[19, "sma_20"])

    def test_sma_50_nan_before_50_rows(self):
        df = make_prices([float(100 + i) for i in range(60)])
        result = calculate_indicators(df)
        assert result.loc[:48, "sma_50"].isna().all()
        assert not math.isnan(result.loc[49, "sma_50"])

    def test_sma_20_value_correct(self):
        # Flat price of 100 → SMA should equal 100
        df = make_prices([100.0] * 25)
        result = calculate_indicators(df)
        assert abs(result.loc[24, "sma_20"] - 100.0) < 1e-6

    def test_ema_starts_from_first_row(self):
        df = make_prices([100.0] * 20)
        result = calculate_indicators(df)
        # EMA has no min_periods requirement — should have values from row 0
        assert not result["ema_12"].isna().any()
        assert not result["ema_26"].isna().any()

    def test_rsi_bounded_between_0_and_100(self):
        closes = [float(100 + i % 7 * (-1) ** i) for i in range(60)]
        df = make_prices(closes)
        result = calculate_indicators(df)
        valid_rsi = result["rsi_14"].dropna()
        assert (valid_rsi >= 0).all() and (valid_rsi <= 100).all()

    def test_volatility_nan_before_30_rows(self):
        df = make_prices([float(100 + i) for i in range(40)])
        result = calculate_indicators(df)
        # volatility_30 needs 30 return values → NaN until row 30
        assert result.loc[:29, "volatility_30"].isna().all()
        assert not math.isnan(result.loc[30, "volatility_30"])

    def test_empty_input_returns_empty(self):
        result = calculate_indicators(pd.DataFrame())
        assert result.empty

    def test_output_has_expected_columns(self):
        df = make_prices([100.0] * 10)
        result = calculate_indicators(df)
        expected = {"date", "daily_return", "sma_20", "sma_50",
                    "ema_12", "ema_26", "rsi_14", "volatility_30"}
        assert expected.issubset(set(result.columns))
