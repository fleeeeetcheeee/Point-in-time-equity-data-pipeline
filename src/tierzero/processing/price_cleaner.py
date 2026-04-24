"""
Price cleaning and point-in-time split adjustment.

Why PIT-correct adjustment matters
------------------------------------
A 4-for-1 split on 2020-08-31 (Apple) changes the adjustment factor for every
prior date. If you query (AAPL, 2020-01-15) *after* the split, the raw close
of ~$300 should be divided by 4 to give a split-adjusted ~$75.

But if you are simulating what was observable on 2020-01-15 — before the split
was announced — the $300 price is the correct unadjusted close and no split
adjustment should be applied.

compute_adjustment_factors(splits, as_of_date) enforces this by only using
splits whose date <= as_of_date. This means a backtest running on 2020-01-15
will see the unadjusted $300 price, and a backtest running on 2020-09-01 will
see the split-adjusted $75. Both are correct for their respective dates.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


class PriceCleaner:
    """
    Cleans raw OHLCV data and computes point-in-time split adjustment factors.

    Usage::

        cleaner = PriceCleaner()
        clean_df = cleaner.clean(raw_df, ticker="AAPL")
        factor = cleaner.get_adjustment_factor(splits_df, as_of_date, price_date)
    """

    # A single-day price move larger than this that is NOT explained by a
    # recorded split is flagged as suspicious.
    PRICE_GAP_THRESHOLD = 0.50

    def clean(self, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """
        Clean a raw OHLCV DataFrame returned by yfinance.

        Actions taken:
        - Sort by date, drop rows with null or non-positive Close.
        - Flag zero-volume days (halted / illiquid — kept but marked).
        - Flag unexplained large price gaps (possible data error or missed split).
        - Add a ticker column.

        Returns a cleaned DataFrame with additional boolean flag columns.
        """
        df = df.copy()
        df = df[df.index.notna()].sort_index()

        # Drop rows with missing or non-positive close
        df = df[df["Close"].notna() & (df["Close"] > 0)].copy()

        if df.empty:
            log.warning("%s: no valid price rows after cleaning.", ticker)
            return df

        df["volume_zero"] = df["Volume"] == 0

        # Detect price gaps that are not explained by a recorded split
        daily_return = df["Close"].pct_change().abs()
        has_split = df.get("Stock Splits", pd.Series(0, index=df.index)) != 0
        df["price_gap_flag"] = (daily_return > self.PRICE_GAP_THRESHOLD) & (~has_split)

        df["ticker"] = ticker
        return df

    def compute_adjustment_factors(
        self,
        splits: pd.DataFrame,
        as_of_date: pd.Timestamp,
    ) -> pd.Series:
        """
        Compute the cumulative price adjustment factor for each split event,
        using only splits that were known as of as_of_date.

        Returns a pd.Series indexed by split date with the cumulative factor
        that should be applied to prices on or before that date.

        A price on date D should be divided by the factor corresponding to the
        most recent split on or after D (i.e. adjustments propagate backward).

        Example
        -------
        Splits: 2020-08-31 (4:1), 2014-06-09 (7:1)
        as_of_date = 2021-01-01  → both splits are known

        A price from 2013-01-01 needs both adjustments:  ÷ (4 × 7) = ÷ 28
        A price from 2015-01-01 needs only the 2020 split: ÷ 4
        A price from 2021-01-01 needs no adjustment: ÷ 1
        """
        if splits.empty:
            return pd.Series(dtype=float)

        # Only use splits that were known by as_of_date
        known = splits[splits["date"] <= as_of_date].copy()
        if known.empty:
            return pd.Series(dtype=float)

        known = known.sort_values("date", ascending=False).reset_index(drop=True)

        # Build cumulative factor: multiply ratios from most recent backward
        factors = pd.Series(index=known["date"], dtype=float)
        cumulative = 1.0
        for _, row in known.iterrows():
            cumulative *= float(row["split_ratio"])
            factors[row["date"]] = cumulative

        return factors

    def get_adjusted_close(
        self,
        unadj_close: float,
        price_date: pd.Timestamp,
        factors: pd.Series,
    ) -> float:
        """
        Return the split-adjusted close for a single price on price_date,
        given a factors Series from compute_adjustment_factors.

        The correct factor is the product of all splits that occurred *after*
        price_date (i.e. the cumulative factor at the earliest split date
        that is still >= price_date).
        """
        if factors.empty:
            return unadj_close

        # Find splits that happened after price_date — those are the ones
        # that retroactively rescale the historical price
        future_splits = factors[factors.index > price_date]
        if future_splits.empty:
            return unadj_close

        # The relevant factor is the one from the earliest post-price-date split
        # (which already compounds all later splits via cumulative calculation)
        factor = future_splits.iloc[-1]
        return unadj_close / factor

    def to_processed_df(
        self,
        raw_df: pd.DataFrame,
        splits: pd.DataFrame,
        ticker: str,
    ) -> pd.DataFrame:
        """
        Produce the final processed price DataFrame with both unadjusted and
        split-adjusted (as-of today) closes, plus cleaned flag columns.

        Note: the 'close_adj' column here is adjusted using ALL known splits.
        For a truly PIT-correct adjusted close as of a historical date, use
        get_adjusted_close() with a date-restricted factors Series.
        """
        df = self.clean(raw_df, ticker)
        if df.empty:
            return df

        factors = self.compute_adjustment_factors(
            splits, as_of_date=pd.Timestamp.now()
        )

        df = df.rename(columns={"Close": "close_unadj"})

        df["close_adj"] = df.apply(
            lambda row: self.get_adjusted_close(
                row["close_unadj"],
                pd.Timestamp(row.name),
                factors,
            ),
            axis=1,
        )

        return df.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Volume": "volume",
            }
        )
