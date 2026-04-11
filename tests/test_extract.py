"""
Tests for src/extract.py

Uses unittest.mock to avoid real API calls.
"""

from datetime import date
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from src.extract import extract_prices, extract_macro


START = date(2024, 1, 1)
END   = date(2024, 3, 31)


# ── extract_prices ─────────────────────────────────────────────────────────────

class TestExtractPrices:

    def _mock_yf_response(self) -> pd.DataFrame:
        """Minimal DataFrame that mimics yfinance output."""
        dates = pd.date_range("2024-01-02", periods=5, freq="B")
        df = pd.DataFrame({
            ("Open",      "AAPL"): [185.0] * 5,
            ("High",      "AAPL"): [187.0] * 5,
            ("Low",       "AAPL"): [183.0] * 5,
            ("Close",     "AAPL"): [186.0] * 5,
            ("Adj Close", "AAPL"): [186.0] * 5,
            ("Volume",    "AAPL"): [50_000_000] * 5,
        }, index=dates)
        df.columns = pd.MultiIndex.from_tuples(df.columns)
        df.index.name = "Date"
        return df

    @patch("src.extract.yf.download")
    def test_returns_dataframe_with_correct_columns(self, mock_dl):
        mock_dl.return_value = self._mock_yf_response()
        result = extract_prices("AAPL", START, END)
        expected_cols = {"date", "open", "high", "low", "close", "adj_close", "volume"}
        assert expected_cols.issubset(set(result.columns))

    @patch("src.extract.yf.download")
    def test_returns_correct_row_count(self, mock_dl):
        mock_dl.return_value = self._mock_yf_response()
        result = extract_prices("AAPL", START, END)
        assert len(result) == 5

    @patch("src.extract.yf.download")
    def test_date_column_is_date_type(self, mock_dl):
        mock_dl.return_value = self._mock_yf_response()
        result = extract_prices("AAPL", START, END)
        assert isinstance(result["date"].iloc[0], date)

    @patch("src.extract.yf.download")
    def test_returns_empty_df_when_api_returns_empty(self, mock_dl):
        mock_dl.return_value = pd.DataFrame()
        result = extract_prices("INVALID", START, END)
        assert result.empty

    @patch("src.extract.time.sleep")
    @patch("src.extract.yf.download")
    def test_retries_on_exception_and_returns_empty(self, mock_dl, mock_sleep):
        mock_dl.side_effect = Exception("API error")
        result = extract_prices("AAPL", START, END)
        assert result.empty
        assert mock_dl.call_count == 3  # 3 attempts


# ── extract_macro ──────────────────────────────────────────────────────────────

class TestExtractMacro:

    def _mock_fred_series(self) -> pd.Series:
        dates = pd.date_range("2024-01-01", periods=4, freq="QS")
        return pd.Series([25000.0, 25500.0, 26000.0, 26500.0], index=dates)

    @patch("src.extract.FRED_API_KEY", "fake_key_1234")
    @patch("src.extract.Fred")
    def test_returns_dataframe_with_correct_columns(self, mock_fred_cls):
        mock_fred_cls.return_value.get_series.return_value = self._mock_fred_series()
        result = extract_macro("GDP", START, END)
        assert set(result.columns) == {"date", "value"}

    @patch("src.extract.FRED_API_KEY", "fake_key_1234")
    @patch("src.extract.Fred")
    def test_returns_correct_row_count(self, mock_fred_cls):
        mock_fred_cls.return_value.get_series.return_value = self._mock_fred_series()
        result = extract_macro("GDP", START, END)
        assert len(result) == 4

    @patch("src.extract.FRED_API_KEY", "fake_key_1234")
    @patch("src.extract.Fred")
    def test_date_column_is_date_type(self, mock_fred_cls):
        mock_fred_cls.return_value.get_series.return_value = self._mock_fred_series()
        result = extract_macro("GDP", START, END)
        assert isinstance(result["date"].iloc[0], date)

    @patch("src.extract.FRED_API_KEY", "fake_key_1234")
    @patch("src.extract.Fred")
    def test_drops_nan_values(self, mock_fred_cls):
        series = self._mock_fred_series()
        series.iloc[1] = None
        mock_fred_cls.return_value.get_series.return_value = series
        result = extract_macro("GDP", START, END)
        assert len(result) == 3

    @patch("src.extract.FRED_API_KEY", "")
    def test_raises_when_api_key_missing(self):
        with pytest.raises(ValueError, match="FRED_API_KEY"):
            extract_macro("GDP", START, END)

    @patch("src.extract.FRED_API_KEY", "fake_key_1234")
    @patch("src.extract.time.sleep")
    @patch("src.extract.Fred")
    def test_retries_on_exception_and_returns_empty(self, mock_fred_cls, mock_sleep):
        mock_fred_cls.return_value.get_series.side_effect = Exception("timeout")
        result = extract_macro("GDP", START, END)
        assert result.empty
        assert mock_fred_cls.return_value.get_series.call_count == 3
