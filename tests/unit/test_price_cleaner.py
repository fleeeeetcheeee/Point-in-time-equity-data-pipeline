"""
Unit tests for PriceCleaner.

Focus: PIT-correct split adjustment — future splits must not affect
the adjusted price returned for queries before the split date.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from tierzero.processing.price_cleaner import PriceCleaner


class TestComputeAdjustmentFactors:

    def test_no_splits_returns_empty_series(self):
        cleaner = PriceCleaner()
        factors = cleaner.compute_adjustment_factors(
            pd.DataFrame(columns=["date", "split_ratio"]),
            as_of_date=pd.Timestamp("2024-01-01"),
        )
        assert factors.empty

    def test_future_split_excluded_from_pre_split_query(self, sample_splits_df):
        """
        Apple 4:1 split on 2020-08-31.
        A query on 2020-01-15 must not see that split.
        """
        cleaner = PriceCleaner()
        factors = cleaner.compute_adjustment_factors(
            sample_splits_df,
            as_of_date=pd.Timestamp("2020-01-15"),
        )
        # Only the 2014-06-09 split (7:1) is known by 2020-01-15
        split_dates_used = set(factors.index.date) if not factors.empty else set()
        assert date(2020, 8, 31) not in split_dates_used, (
            "The 2020-08-31 split must not appear in a query dated 2020-01-15"
        )

    def test_all_splits_known_after_last_split(self, sample_splits_df):
        """After both splits, both should appear in the factors."""
        cleaner = PriceCleaner()
        factors = cleaner.compute_adjustment_factors(
            sample_splits_df,
            as_of_date=pd.Timestamp("2021-01-01"),
        )
        assert not factors.empty
        # Both 2014 and 2020 splits should be in the factors
        split_dates_used = {d.date() for d in factors.index}
        assert date(2014, 6, 9) in split_dates_used
        assert date(2020, 8, 31) in split_dates_used

    def test_cumulative_factor_compounds_correctly(self):
        """
        Single 4:1 split. A price from before the split should be divided by 4.
        """
        splits = pd.DataFrame({
            "date": pd.to_datetime(["2020-08-31"]),
            "split_ratio": [4.0],
            "ticker": ["TEST"],
        })
        cleaner = PriceCleaner()
        factors = cleaner.compute_adjustment_factors(
            splits,
            as_of_date=pd.Timestamp("2021-01-01"),
        )
        price_date = pd.Timestamp("2019-01-01")
        unadj = 400.0
        adj = cleaner.get_adjusted_close(unadj, price_date, factors)
        assert adj == pytest.approx(100.0), "400 / 4 = 100"

    def test_post_split_price_not_adjusted(self):
        """A price after the split date should not be divided."""
        splits = pd.DataFrame({
            "date": pd.to_datetime(["2020-08-31"]),
            "split_ratio": [4.0],
            "ticker": ["TEST"],
        })
        cleaner = PriceCleaner()
        factors = cleaner.compute_adjustment_factors(
            splits,
            as_of_date=pd.Timestamp("2021-01-01"),
        )
        price_date = pd.Timestamp("2020-09-01")
        unadj = 125.0
        adj = cleaner.get_adjusted_close(unadj, price_date, factors)
        assert adj == pytest.approx(125.0), "Post-split price should be unchanged"


class TestClean:

    def _make_raw_df(self) -> pd.DataFrame:
        idx = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
        return pd.DataFrame(
            {
                "Open": [185.0, 186.0, 0.0],
                "High": [187.0, 188.0, 0.0],
                "Low": [184.0, 185.0, 0.0],
                "Close": [186.0, 187.0, -1.0],   # last row has bad close
                "Volume": [50_000_000, 0, 30_000_000],
                "Stock Splits": [0.0, 0.0, 0.0],
            },
            index=idx,
        )

    def test_non_positive_close_removed(self):
        df = self._make_raw_df()
        cleaned = PriceCleaner().clean(df, "TEST")
        # clean() keeps yfinance's column names; the rename to the storage
        # schema (close_unadj, open, high, ...) happens in to_processed_df().
        assert len(cleaned) == 2, "The row with Close = -1.0 should be dropped"
        assert all(cleaned["Close"] > 0)

    def test_zero_volume_flagged(self):
        df = self._make_raw_df()
        # Remove bad row first so we can check volume flag
        df = df[df["Close"] > 0]
        cleaned = PriceCleaner().clean(df, "TEST")
        assert cleaned["volume_zero"].any()

    def test_ticker_column_added(self):
        df = self._make_raw_df()
        df = df[df["Close"] > 0]
        cleaned = PriceCleaner().clean(df, "AAPL")
        assert (cleaned["ticker"] == "AAPL").all()
