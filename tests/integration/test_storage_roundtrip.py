"""
End-to-end round-trip through the real storage stack.

Why this file exists
--------------------
test_no_lookahead.py deliberately tests filtering logic directly, without a
running DuckDB instance, to stay fast and deterministic. That keeps it fast but
leaves a gap: its fixtures are hand-written *already in the storage schema*, so
nothing there exercises the seam between the layer that produces columns
(processing) and the layers that consume them (storage, reader, lookup).

That gap is not hypothetical. `PriceCleaner.clean()` emits yfinance's
capitalized names (`Close`) and only `to_processed_df()` renames them to the
storage schema (`close_unadj`). A drift between what processing emits and what
PRICE_SCHEMA / lookup.py's SQL expect would pass every test in
test_no_lookahead.py and then fail on the first real bootstrap run.

These tests drive the actual path the scripts use:

    raw OHLCV → to_processed_df → ParquetWriter → DuckDB DataReader
              → PointInTimeLookup.get()

No mocks, no hand-built storage-schema fixtures — the only input is a raw
yfinance-shaped frame.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from tierzero.pit.lookup import PointInTimeLookup
from tierzero.processing.price_cleaner import PriceCleaner
from tierzero.storage.reader import DataReader
from tierzero.storage.writer import PRICE_SCHEMA, ParquetWriter


TICKER = "AAPL"
CIK = 320193


def _raw_ohlcv() -> pd.DataFrame:
    """
    A yfinance-shaped frame: DatetimeIndex, capitalized columns.

    Thu 2024-01-04 → Fri 2024-01-05 → Mon 2024-01-08.
    The weekend gap lets us test last-trading-day fallback through real SQL.
    """
    idx = pd.to_datetime(["2024-01-04", "2024-01-05", "2024-01-08"])
    return pd.DataFrame(
        {
            "Open": [182.0, 184.0, 186.0],
            "High": [183.5, 186.0, 188.0],
            "Low": [181.0, 183.0, 185.0],
            "Close": [182.5, 185.0, 187.0],
            "Volume": [40_000_000, 50_000_000, 55_000_000],
            "Stock Splits": [0.0, 0.0, 0.0],
        },
        index=idx,
    )


def _to_storage_frame(processed: pd.DataFrame) -> pl.DataFrame:
    """
    Mirror of bootstrap.py:153-159 / daily_update.py — the reshape that sits
    between to_processed_df() and the writer.

    This duplication is deliberate and is itself the thing under test: if the
    scripts' column list and the cleaner's output ever diverge, this raises
    here rather than four hours into a bootstrap run.
    """
    processed = processed.reset_index().rename(columns={"index": "date"})
    processed["date"] = pd.to_datetime(processed["date"]).dt.date
    return pl.from_pandas(
        processed[
            ["date", "ticker", "open", "high", "low",
             "close_unadj", "close_adj", "volume",
             "volume_zero", "price_gap_flag"]
        ].dropna(subset=["close_unadj"])
    )


@pytest.fixture
def no_splits() -> pd.DataFrame:
    return pd.DataFrame(columns=["date", "split_ratio", "ticker"])


@pytest.fixture
def storage_frame(no_splits) -> pl.DataFrame:
    processed = PriceCleaner().to_processed_df(_raw_ohlcv(), no_splits, TICKER)
    return _to_storage_frame(processed)


@pytest.fixture
def data_root(tmp_path: Path, storage_frame, sample_fundamentals_df) -> Path:
    """A populated data root written through the real ParquetWriter."""
    writer = ParquetWriter()
    writer.write_prices(storage_frame, tmp_path / "processed" / "prices")
    writer.write_fundamentals(
        sample_fundamentals_df,
        "2024q1",
        tmp_path / "processed" / "fundamentals" / "as_filed",
    )
    return tmp_path


@pytest.fixture
def pit_lookup(data_root: Path) -> PointInTimeLookup:
    membership = pd.DataFrame(
        {
            "ticker": [TICKER],
            "entry_date": pd.to_datetime(["1982-11-30"]),
            "exit_date": pd.to_datetime([pd.NaT]),
        }
    )
    ticker_cik = pd.DataFrame({"ticker": [TICKER], "cik": [CIK]})
    return PointInTimeLookup(DataReader(data_root), membership, ticker_cik)


# ======================================================================
# The processing → storage seam
# ======================================================================

class TestSchemaContract:

    def test_processed_columns_match_price_schema(self, storage_frame):
        """
        The columns to_processed_df() produces must be exactly the columns
        PRICE_SCHEMA declares. This is the assertion that catches rename drift.
        """
        assert set(storage_frame.columns) == set(PRICE_SCHEMA), (
            "Cleaner output and PRICE_SCHEMA have diverged: "
            f"missing={set(PRICE_SCHEMA) - set(storage_frame.columns)}, "
            f"unexpected={set(storage_frame.columns) - set(PRICE_SCHEMA)}"
        )

    def test_bad_close_rows_dropped_before_storage(self, no_splits):
        """A non-positive close must not survive into the store."""
        raw = _raw_ohlcv()
        raw.loc[pd.Timestamp("2024-01-05"), "Close"] = -1.0

        processed = PriceCleaner().to_processed_df(raw, no_splits, TICKER)
        frame = _to_storage_frame(processed)

        assert frame.height == 2
        assert all(c > 0 for c in frame["close_unadj"].to_list())


# ======================================================================
# Writer → reader round trip
# ======================================================================

class TestParquetRoundTrip:

    def test_hive_partitions_created_per_date(self, data_root: Path):
        partitions = sorted(
            p.name for p in (data_root / "processed" / "prices").iterdir() if p.is_dir()
        )
        assert partitions == ["date=2024-01-04", "date=2024-01-05", "date=2024-01-08"]

    def test_no_tmp_files_left_behind(self, data_root: Path):
        """Atomic writes must leave no .tmp.parquet artifacts on success."""
        leftovers = list(data_root.rglob("*.tmp.parquet"))
        assert leftovers == [], f"Atomic write left temp files behind: {leftovers}"

    def test_values_survive_the_round_trip(self, data_root: Path, storage_frame):
        reader = DataReader(data_root)
        back = reader.query(
            f"SELECT date, close_unadj, volume FROM prices "
            f"WHERE ticker = '{TICKER}' ORDER BY date"
        )

        assert back.height == storage_frame.height
        assert back["close_unadj"].to_list() == pytest.approx([182.5, 185.0, 187.0])
        assert back["volume"].to_list() == [40_000_000, 50_000_000, 55_000_000]

    def test_reader_sees_every_schema_column(self, data_root: Path):
        """Every column lookup.py's SQL selects must exist in the view."""
        reader = DataReader(data_root)
        back = reader.query(f"SELECT * FROM prices WHERE ticker = '{TICKER}' LIMIT 1")
        for column in PRICE_SCHEMA:
            assert column in back.columns, f"'{column}' missing from the prices view"


# ======================================================================
# Full stack through PointInTimeLookup
# ======================================================================

class TestLookupOverRealStorage:

    def test_snapshot_returns_stored_price(self, pit_lookup: PointInTimeLookup):
        snap = pit_lookup.get(TICKER, date(2024, 1, 5))

        assert snap.has_price
        assert snap.close == pytest.approx(185.0)
        assert snap.open == pytest.approx(184.0)
        assert snap.volume == 50_000_000
        assert snap.is_trading_day

    def test_weekend_query_falls_back_to_friday(self, pit_lookup: PointInTimeLookup):
        """
        Saturday 2024-01-06 has no row. The real SQL path must return Friday's
        close — not Monday's, which would be lookahead.
        """
        snap = pit_lookup.get(TICKER, date(2024, 1, 6))

        assert snap.close == pytest.approx(185.0), "Must use Friday, not Monday"
        assert not snap.is_trading_day
        assert any("last available" in w for w in snap.data_warnings)

    def test_no_future_price_leaks_into_snapshot(self, pit_lookup: PointInTimeLookup):
        """A query before any data exists must return no price at all."""
        snap = pit_lookup.get(TICKER, date(2024, 1, 3))
        assert not snap.has_price
        assert snap.close is None

    def test_membership_resolved_through_stack(self, pit_lookup: PointInTimeLookup):
        snap = pit_lookup.get(TICKER, date(2024, 1, 5))
        assert snap.in_sp500

    def test_unknown_ticker_returns_empty_snapshot(self, pit_lookup: PointInTimeLookup):
        snap = pit_lookup.get("NOSUCHTICKER", date(2024, 1, 5))
        assert not snap.has_price
        assert not snap.in_sp500
