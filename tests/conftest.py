"""
Shared pytest fixtures for unit and integration tests.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import polars as pl
import pytest


# ------------------------------------------------------------------
# Sample EDGAR sub.txt equivalent (two Apple filings)
# ------------------------------------------------------------------

@pytest.fixture
def sample_sub_rows() -> list[dict]:
    """Two filings: Q3 FY2023 (filed Nov 2023) and Q4 FY2023 (filed Feb 2024)."""
    return [
        {
            "adsh": "Q3-2023",
            "cik": 320193,
            "name": "Apple Inc.",
            "form": "10-Q",
            "filed": 20231103,
            "period": 20230930,
            "prevrpt": 0,
        },
        {
            "adsh": "Q4-2023",
            "cik": 320193,
            "name": "Apple Inc.",
            "form": "10-Q",
            "filed": 20240202,
            "period": 20231230,
            "prevrpt": 0,
        },
    ]


@pytest.fixture
def sample_fundamentals_df() -> pl.DataFrame:
    """
    Minimal fundamentals DataFrame mirroring the EdgarParser output schema.

    Q3 revenue filed 2023-11-03, Q4 revenue filed 2024-02-02.
    Values differ so tests can detect which one was returned.
    """
    return pl.DataFrame(
        {
            "adsh": ["Q3-2023", "Q3-2023", "Q4-2023", "Q4-2023"],
            "cik": [320193, 320193, 320193, 320193],
            "name": ["Apple Inc."] * 4,
            "tag": ["Revenues", "Assets", "Revenues", "Assets"],
            "filed_date": [
                date(2023, 11, 3),
                date(2023, 11, 3),
                date(2024, 2, 2),
                date(2024, 2, 2),
            ],
            "period_date": [
                date(2023, 9, 30),
                date(2023, 9, 30),
                date(2023, 12, 30),
                date(2023, 12, 30),
            ],
            "form": ["10-Q", "10-Q", "10-Q", "10-Q"],
            "is_amendment": [False, False, False, False],
            "qtrs": [1, 0, 1, 0],
            "value": [
                89_498_000_000.0,   # Q3 revenue
                352_583_000_000.0,  # Q3 assets
                119_575_000_000.0,  # Q4 revenue  ← must NOT appear before 2024-02-02
                353_514_000_000.0,  # Q4 assets
            ],
            "uom": ["USD", "USD", "USD", "USD"],
        }
    )


@pytest.fixture
def sample_membership_df() -> pd.DataFrame:
    """Small membership timeline for testing."""
    return pd.DataFrame(
        {
            "ticker": ["AAPL", "TESTCO", "TESTCO"],
            "entry_date": pd.to_datetime(["1982-11-30", "2005-01-01", "2010-06-01"]),
            "exit_date": pd.to_datetime([pd.NaT, "2008-01-15", pd.NaT]),
        }
    )


@pytest.fixture
def sample_splits_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2014-06-09", "2020-08-31"]),
            "split_ratio": [7.0, 4.0],
            "ticker": ["AAPL", "AAPL"],
        }
    )
