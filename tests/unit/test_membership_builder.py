"""
Unit tests for MembershipBuilder.

Tests the (ticker, entry_date, exit_date) timeline construction and
the was_member_on / get_constituents_on PIT query methods.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from tierzero.processing.membership_builder import MembershipBuilder


def _make_changes(*rows) -> pd.DataFrame:
    """Helper: build a changes DataFrame from (date_str, added, removed) tuples."""
    records = []
    for date_str, added, removed in rows:
        records.append({
            "date": pd.Timestamp(date_str),
            "ticker_added": added,
            "ticker_removed": removed,
        })
    return pd.DataFrame(records)


class TestBuildTimeline:

    def test_simple_add(self):
        changes = _make_changes(("2010-01-01", "AAPL", None))
        tl = MembershipBuilder().build_timeline(changes)
        assert len(tl) == 1
        row = tl.iloc[0]
        assert row["ticker"] == "AAPL"
        assert row["entry_date"] == pd.Timestamp("2010-01-01")
        assert pd.isna(row["exit_date"])

    def test_add_then_remove(self):
        changes = _make_changes(
            ("2005-01-01", "TESTCO", None),
            ("2008-01-15", None, "TESTCO"),
        )
        tl = MembershipBuilder().build_timeline(changes)
        assert len(tl) == 1
        row = tl.iloc[0]
        assert row["ticker"] == "TESTCO"
        assert row["entry_date"] == pd.Timestamp("2005-01-01")
        assert row["exit_date"] == pd.Timestamp("2008-01-15")

    def test_add_remove_readd(self):
        """A company that leaves and re-enters should have two timeline rows."""
        changes = _make_changes(
            ("2000-01-01", "REJOINER", None),
            ("2005-06-01", None, "REJOINER"),
            ("2010-03-01", "REJOINER", None),
        )
        tl = MembershipBuilder().build_timeline(changes)
        rows = tl[tl["ticker"] == "REJOINER"]
        assert len(rows) == 2, "Should have two membership periods"
        assert rows.iloc[0]["exit_date"] == pd.Timestamp("2005-06-01")
        assert pd.isna(rows.iloc[1]["exit_date"])

    def test_simultaneous_swap(self):
        """One ticker added and another removed on the same date."""
        changes = _make_changes(
            ("2000-01-01", "OLD", None),
            ("2015-06-01", "NEW", "OLD"),
        )
        tl = MembershipBuilder().build_timeline(changes)
        old_row = tl[tl["ticker"] == "OLD"].iloc[0]
        new_row = tl[tl["ticker"] == "NEW"].iloc[0]
        assert old_row["exit_date"] == pd.Timestamp("2015-06-01")
        assert new_row["entry_date"] == pd.Timestamp("2015-06-01")


class TestWasMemberOn:

    def setup_method(self):
        changes = _make_changes(
            ("2005-01-01", "TESTCO", None),
            ("2008-01-15", None, "TESTCO"),
        )
        self.tl = MembershipBuilder().build_timeline(changes)
        self.mb = MembershipBuilder()

    def test_member_during_period(self):
        assert self.mb.was_member_on(self.tl, "TESTCO", pd.Timestamp("2007-12-31"))

    def test_not_member_after_removal(self):
        assert not self.mb.was_member_on(self.tl, "TESTCO", pd.Timestamp("2008-02-01"))

    def test_not_member_before_addition(self):
        assert not self.mb.was_member_on(self.tl, "TESTCO", pd.Timestamp("2004-12-31"))

    def test_member_on_exact_entry_date(self):
        assert self.mb.was_member_on(self.tl, "TESTCO", pd.Timestamp("2005-01-01"))

    def test_not_member_on_exact_exit_date(self):
        """Exit date is exclusive — the company was removed on that date."""
        assert not self.mb.was_member_on(self.tl, "TESTCO", pd.Timestamp("2008-01-15"))

    def test_unknown_ticker_returns_false(self):
        assert not self.mb.was_member_on(self.tl, "UNKNOWN", pd.Timestamp("2006-01-01"))


class TestGetConstituentsOn:

    def test_returns_correct_set(self):
        changes = _make_changes(
            ("2000-01-01", "AAPL", None),
            ("2000-01-01", "MSFT", None),
            ("2005-01-01", None, "MSFT"),
        )
        tl = MembershipBuilder().build_timeline(changes)
        mb = MembershipBuilder()

        before_msft_removal = mb.get_constituents_on(tl, pd.Timestamp("2004-01-01"))
        assert set(before_msft_removal) == {"AAPL", "MSFT"}

        after_msft_removal = mb.get_constituents_on(tl, pd.Timestamp("2006-01-01"))
        assert set(after_msft_removal) == {"AAPL"}
