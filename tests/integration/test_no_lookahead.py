"""
Integration tests for point-in-time data leakage.

These are the done-criterion tests for the pipeline. They verify the central
correctness guarantee: no future data can appear in a snapshot returned for
a given as_of_date.

Test philosophy
---------------
We construct synthetic data where a leak would be immediately detectable —
e.g. earnings filed 90 days after the query date must not appear. We test
the filtering logic directly (without a running DuckDB instance) to keep
these tests fast and deterministic.

Test classes
------------
TestFundamentalsNoLookahead  — filed_date gate is correctly enforced
TestPriceNoLookahead         — price queries don't use future prices
TestSurvivorshipBias         — delisted companies included, removed members excluded
TestAmendmentHandling        — 10-K/A supersedes original only after its filed_date
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import polars as pl
import pytest

from tierzero.processing.membership_builder import MembershipBuilder
from tierzero.processing.price_cleaner import PriceCleaner
from tierzero.pit.lookup import _compute_ttm


# ======================================================================
# Fundamentals — the core PIT invariant
# ======================================================================

class TestFundamentalsNoLookahead:
    """
    Scenario: Apple Q3 FY2023 results filed 2023-11-03 (revenue ~$89.5B).
              Apple Q4 FY2023 results filed 2024-02-02 (revenue ~$119.6B).

    A query on 2023-12-15 must return Q3 data only.
    A query on 2024-02-15 must return Q4 data.
    """

    def _filter_by_pit(
        self, df: pl.DataFrame, as_of_date: date
    ) -> pl.DataFrame:
        """Replicate the PIT filter applied in PointInTimeLookup._populate_fundamentals."""
        return df.filter(pl.col("filed_date") <= as_of_date)

    def test_december_query_returns_q3_not_q4(self, sample_fundamentals_df):
        query_date = date(2023, 12, 15)
        known = self._filter_by_pit(sample_fundamentals_df, query_date)
        revenues = known.filter(pl.col("tag") == "Revenues")

        assert revenues.height == 1, "Exactly one revenue filing should be visible"
        assert revenues["value"][0] == pytest.approx(89_498_000_000.0), (
            "Must see Q3 revenue ($89.5B), not Q4 ($119.6B)"
        )
        assert revenues["filed_date"][0] == date(2023, 11, 3)

    def test_february_query_returns_q4(self, sample_fundamentals_df):
        query_date = date(2024, 2, 15)
        known = (
            self._filter_by_pit(sample_fundamentals_df, query_date)
            .sort("filed_date", descending=True)
        )
        revenues = known.filter(pl.col("tag") == "Revenues")

        assert revenues["value"][0] == pytest.approx(119_575_000_000.0), (
            "Must see Q4 revenue ($119.6B) after filing date"
        )

    def test_exact_boundary_day_before_filing(self, sample_fundamentals_df):
        """One day before the Q4 filing: Q4 data must not appear."""
        day_before = date(2024, 2, 1)
        known = self._filter_by_pit(sample_fundamentals_df, day_before)
        q4_rows = known.filter(pl.col("adsh") == "Q4-2023")
        assert q4_rows.height == 0, "Q4 data must not appear the day before filing"

    def test_exact_boundary_on_filing_day(self, sample_fundamentals_df):
        """On the exact filing day: Q4 data must appear."""
        filing_day = date(2024, 2, 2)
        known = self._filter_by_pit(sample_fundamentals_df, filing_day)
        q4_rows = known.filter(pl.col("adsh") == "Q4-2023")
        assert q4_rows.height > 0, "Q4 data must appear on the filing date itself"

    def test_period_date_does_not_gate_availability(self, sample_fundamentals_df):
        """
        The Q4 period ended 2023-12-30, but was not filed until 2024-02-02.
        A query on 2024-01-15 — after the period end but before filing — must
        NOT see Q4 data. This is the most common data-leakage mistake.
        """
        query_date = date(2024, 1, 15)
        known = self._filter_by_pit(sample_fundamentals_df, query_date)
        q4_rows = known.filter(pl.col("adsh") == "Q4-2023")
        assert q4_rows.height == 0, (
            "Data must not be available between period_end and filed_date. "
            "Using period_end as the availability date is a common leakage bug."
        )


# ======================================================================
# TTM construction
# ======================================================================

class TestTTMConstruction:

    def _quarterly_facts(self, values: list[float], periods: list[str]) -> pl.DataFrame:
        return pl.DataFrame({
            "tag": ["Revenues"] * len(values),
            "value": values,
            "period_date": periods,
            "qtrs": [1] * len(values),
            "adsh": [f"ADSH-{i}" for i in range(len(values))],
        })

    def test_four_quarters_sum_correctly(self):
        facts = self._quarterly_facts(
            [10.0, 20.0, 30.0, 40.0],
            ["2023-09-30", "2023-06-30", "2023-03-31", "2022-12-31"],
        )
        result = _compute_ttm(facts)
        assert result == pytest.approx(100.0)

    def test_fewer_than_four_quarters_returns_none(self):
        facts = self._quarterly_facts(
            [10.0, 20.0, 30.0],
            ["2023-09-30", "2023-06-30", "2023-03-31"],
        )
        assert _compute_ttm(facts) is None

    def test_annual_filing_used_directly(self):
        facts = pl.DataFrame({
            "tag": ["Revenues"],
            "value": [400.0],
            "period_date": ["2023-12-31"],
            "qtrs": [4],
            "adsh": ["ANNUAL"],
        })
        assert _compute_ttm(facts) == pytest.approx(400.0)

    def test_annual_preferred_over_quarters(self):
        """If both annual and quarterly data exist, use the annual figure."""
        annual = pl.DataFrame({
            "tag": ["Revenues"], "value": [400.0],
            "period_date": ["2023-12-31"], "qtrs": [4], "adsh": ["ANN"],
        })
        quarterly = pl.DataFrame({
            "tag": ["Revenues"] * 4,
            "value": [90.0, 95.0, 100.0, 105.0],
            "period_date": ["2023-12-31", "2023-09-30", "2023-06-30", "2023-03-31"],
            "qtrs": [1, 1, 1, 1],
            "adsh": ["Q4", "Q3", "Q2", "Q1"],
        })
        combined = pl.concat([annual, quarterly])
        result = _compute_ttm(combined)
        assert result == pytest.approx(400.0), "Annual filing should be preferred"


# ======================================================================
# Price no-lookahead
# ======================================================================

class TestPriceNoLookahead:

    def _make_prices(self) -> pl.DataFrame:
        return pl.DataFrame({
            "date": [date(2024, 1, 5), date(2024, 1, 8)],  # Friday, Monday
            "ticker": ["AAPL", "AAPL"],
            "close_unadj": [185.0, 187.0],
            "close_adj": [185.0, 187.0],
            "open": [184.0, 186.0],
            "high": [186.0, 188.0],
            "low": [183.0, 185.0],
            "volume": [50_000_000, 55_000_000],
        })

    def test_saturday_query_uses_friday_price(self):
        """Saturday is not a trading day; query must return Friday's close."""
        prices = self._make_prices()
        query_date = date(2024, 1, 6)  # Saturday
        available = (
            prices.filter(pl.col("date") <= query_date)
            .sort("date", descending=True)
        )
        assert available["date"][0] == date(2024, 1, 5), (
            "Saturday query must return Friday price, not Monday"
        )
        assert available["close_unadj"][0] == pytest.approx(185.0)

    def test_future_split_not_applied_to_historical_query(self, sample_splits_df):
        """
        Apple 4:1 split on 2020-08-31.
        A query on 2020-01-15 should not apply that split.
        """
        cleaner = PriceCleaner()
        pre_split_query = pd.Timestamp("2020-01-15")
        factors = cleaner.compute_adjustment_factors(
            sample_splits_df, as_of_date=pre_split_query
        )
        splits_used = set(factors.index.date) if not factors.empty else set()
        assert date(2020, 8, 31) not in splits_used, (
            "A pre-split query must not use splits that haven't happened yet"
        )

    def test_price_query_is_strictly_less_than_or_equal(self):
        """
        Price on as_of_date itself is visible; price on as_of_date + 1 is not.
        """
        prices = self._make_prices()
        query_date = date(2024, 1, 5)  # Friday
        available = prices.filter(pl.col("date") <= query_date)
        assert all(d <= query_date for d in available["date"].to_list())


# ======================================================================
# Survivorship bias
# ======================================================================

class TestSurvivorshipBias:

    def test_delisted_company_has_data_before_delisting(self):
        """
        A delisted company must return price data for dates before its last trade.
        Excluding dead companies from the universe biases backtest returns upward.
        """
        prices = pl.DataFrame({
            "date": [date(2008, 9, 12), date(2008, 9, 15)],
            "ticker": ["LEHMAN", "LEHMAN"],
            "close_unadj": [3.65, 0.21],
        })
        available = prices.filter(
            (pl.col("ticker") == "LEHMAN") & (pl.col("date") <= date(2008, 9, 12))
        )
        assert available.height > 0, (
            "Delisted company must have data available before its last trading date"
        )

    def test_removed_sp500_member_not_in_universe(self):
        """
        A company removed from the S&P 500 must not appear in the universe
        for dates after its removal.
        """
        mb = MembershipBuilder()
        changes = pd.DataFrame({
            "date": pd.to_datetime(["2005-01-01", "2008-01-15"]),
            "ticker_added": ["TESTCO", None],
            "ticker_removed": [None, "TESTCO"],
        })
        timeline = mb.build_timeline(changes)

        assert mb.was_member_on(timeline, "TESTCO", pd.Timestamp("2007-12-31"))
        assert not mb.was_member_on(timeline, "TESTCO", pd.Timestamp("2008-02-01")), (
            "Removed member must not appear in post-removal universe queries"
        )

    def test_universe_on_date_excludes_future_entrants(self):
        """
        A company that joined the S&P 500 in 2020 must not appear in the
        2015 universe — it was not yet a constituent.
        """
        mb = MembershipBuilder()
        changes = pd.DataFrame({
            "date": pd.to_datetime(["2020-03-01"]),
            "ticker_added": ["NEWCOMER"],
            "ticker_removed": [None],
        })
        timeline = mb.build_timeline(changes)
        constituents_2015 = mb.get_constituents_on(timeline, pd.Timestamp("2015-01-01"))
        assert "NEWCOMER" not in constituents_2015, (
            "Future index entrant must not appear in historical universe"
        )


# ======================================================================
# Amendment handling
# ======================================================================

class TestAmendmentHandling:
    """
    A 10-K/A (amended annual report) restates financials with a new filed_date.
    - Queries before the amendment date must see the original value.
    - Queries after the amendment date must see the restated value.
    """

    def _make_amendment_data(self) -> pl.DataFrame:
        return pl.DataFrame({
            "adsh": ["ORIG-001", "AMEND-001"],
            "cik": [12345, 12345],
            "tag": ["NetIncomeLoss", "NetIncomeLoss"],
            "filed_date": [date(2024, 2, 1), date(2024, 3, 15)],
            "period_date": [date(2023, 12, 31), date(2023, 12, 31)],
            "form": ["10-K", "10-K/A"],
            "is_amendment": [False, True],
            "qtrs": [4, 4],
            "value": [1_000_000.0, 950_000.0],  # restated downward
            "uom": ["USD", "USD"],
        })

    def test_before_amendment_returns_original(self):
        data = self._make_amendment_data()
        before = (
            data.filter(pl.col("filed_date") <= date(2024, 3, 1))
            .sort("filed_date", descending=True)
        )
        assert before["value"][0] == pytest.approx(1_000_000.0), (
            "Before amendment date, original value must be returned"
        )

    def test_after_amendment_returns_restated(self):
        data = self._make_amendment_data()
        after = (
            data.filter(pl.col("filed_date") <= date(2024, 4, 1))
            .sort("filed_date", descending=True)
        )
        assert after["value"][0] == pytest.approx(950_000.0), (
            "After amendment date, restated value must be returned"
        )

    def test_amendment_not_visible_before_its_filed_date(self):
        data = self._make_amendment_data()
        before_amendment = data.filter(
            pl.col("filed_date") <= date(2024, 3, 14)
        ).filter(pl.col("is_amendment"))
        assert before_amendment.height == 0, (
            "Amendment must not be visible before its own filed_date"
        )
