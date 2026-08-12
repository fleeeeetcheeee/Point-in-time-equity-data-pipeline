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
def empty_root(tmp_path: Path, sample_fundamentals_df) -> Path:
    """
    A data root with fundamentals but no prices, for tests that write their own.

    DataReader registers a view per dataset at construction and errors if a
    glob matches nothing, so fundamentals have to exist even when the test is
    only about prices.
    """
    ParquetWriter().write_fundamentals(
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


class TestSchemaEnforcement:
    """
    PRICE_SCHEMA is documented as enforced on write. These tests are what makes
    that claim true — before them the writer only checked for a 'date' column.
    """

    def test_missing_column_is_rejected(self, tmp_path, storage_frame):
        crippled = storage_frame.drop("close_adj")

        with pytest.raises(ValueError, match="close_adj"):
            ParquetWriter().write_prices(crippled, tmp_path)

    def test_unexpected_column_is_rejected(self, tmp_path, storage_frame):
        """
        An extra column usually means a rename landed in processing without the
        schema following it — the exact drift this file was written to catch.
        """
        extra = storage_frame.with_columns(pl.lit(1.0).alias("close_adjusted"))

        with pytest.raises(ValueError, match="close_adjusted"):
            ParquetWriter().write_prices(extra, tmp_path)

    def test_uncastable_column_is_rejected(self, tmp_path, storage_frame):
        garbage = storage_frame.with_columns(pl.lit("not a price").alias("close_unadj"))

        with pytest.raises(ValueError, match="could not be cast"):
            ParquetWriter().write_prices(garbage, tmp_path)

    def test_nothing_is_written_when_validation_fails(self, tmp_path, storage_frame):
        """Validation must happen before any partition is created."""
        with pytest.raises(ValueError):
            ParquetWriter().write_prices(storage_frame.drop("volume"), tmp_path)

        assert list(tmp_path.iterdir()) == []

    def test_castable_width_mismatch_is_accepted(self, tmp_path, storage_frame):
        """
        pandas picks integer and boolean widths based on nulls, so Int32 volume
        is a normal upstream outcome rather than a bug. It should be cast, not
        rejected.
        """
        narrowed = storage_frame.with_columns(pl.col("volume").cast(pl.Int32))

        ParquetWriter().write_prices(narrowed, tmp_path)

        written = pl.read_parquet(tmp_path / "date=2024-01-04" / "data.parquet")
        assert written["volume"].dtype == pl.Int64

    def test_column_order_is_canonical_on_disk(self, tmp_path, storage_frame):
        shuffled = storage_frame.select(reversed(storage_frame.columns))

        ParquetWriter().write_prices(shuffled, tmp_path)

        written = pl.read_parquet(tmp_path / "date=2024-01-04" / "data.parquet")
        assert written.columns == list(PRICE_SCHEMA)


# ======================================================================
# Writer → reader round trip
# ======================================================================

class TestMultiTickerPartitions:
    """
    A date partition holds every ticker that traded that day.

    Every test above this point uses a single ticker, and that blind spot cost
    real data: the writer used a fixed `data.parquet` per partition, so the
    bootstrap's per-ticker write loop silently overwrote each ticker with the
    next. A 19-ticker smoke run put 3 tickers in the store and nobody's test
    failed. These tests write more than one ticker, which is the only way to
    see it.
    """

    def test_second_write_does_not_evict_the_first(self, tmp_path, storage_frame):
        writer = ParquetWriter()
        other = storage_frame.with_columns(pl.lit("MSFT").alias("ticker"))

        writer.write_prices(storage_frame, tmp_path, part_name="part-0000")
        writer.write_prices(other, tmp_path, part_name="part-0001")

        back = pl.read_parquet(tmp_path / "date=2024-01-04" / "*.parquet")
        assert set(back["ticker"]) == {TICKER, "MSFT"}

    def test_reader_sees_every_ticker_in_a_partition(
        self, empty_root, storage_frame
    ):
        """The DuckDB glob must pick up all parts, not just the first."""
        writer = ParquetWriter()
        for i, tkr in enumerate(["MSFT", "GOOG"]):
            writer.write_prices(
                storage_frame.with_columns(pl.lit(tkr).alias("ticker")),
                empty_root / "processed" / "prices",
                part_name=f"part-{i:04d}",
            )

        reader = DataReader(empty_root)
        got = reader.query("SELECT DISTINCT ticker FROM prices ORDER BY ticker")

        assert got["ticker"].to_list() == ["GOOG", "MSFT"]

    def test_reusing_a_part_name_overwrites_rather_than_duplicates(
        self, empty_root, storage_frame
    ):
        """Re-running a bootstrap must be idempotent, not additive."""
        writer = ParquetWriter()
        writer.write_prices(storage_frame, empty_root / "processed" / "prices",
                            part_name="part-0000")
        writer.write_prices(storage_frame, empty_root / "processed" / "prices",
                            part_name="part-0000")

        reader = DataReader(empty_root)
        n = reader.query(
            f"SELECT COUNT(*) AS n FROM prices WHERE ticker = '{TICKER}'"
        )["n"][0]

        assert n == storage_frame.height, "Re-running duplicated rows"

    def test_default_part_name_is_stable(self, tmp_path, storage_frame):
        """Callers that don't pass a part name still get idempotent writes."""
        writer = ParquetWriter()
        writer.write_prices(storage_frame, tmp_path)
        writer.write_prices(storage_frame, tmp_path)

        parts = list((tmp_path / "date=2024-01-04").glob("*.parquet"))
        assert len(parts) == 1


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


class TestUniverseQueryCost:
    """
    get_universe() is called once per rebalance date by a backtester, so its
    cost per call is multiplied by thousands.

    It was originally `[self.get(t) for t in tickers]`, and each get() ran two
    unbounded scans of the price store. On a 239-name universe over a store
    with 8,459 date partitions that measured 121 seconds for one date. These
    tests pin the batching that replaced it: the guard is the *number of
    queries*, because that is the property that regressed, and a timing
    assertion would be flaky.
    """

    @pytest.fixture
    def counting_lookup(self, data_root: Path):
        """A lookup whose reader records every query it issues."""
        reader = DataReader(data_root)
        queries: list[str] = []

        original = reader.query

        def counting_query(sql: str):
            queries.append(sql)
            return original(sql)

        reader.query = counting_query

        membership = pd.DataFrame({
            "ticker": [TICKER, "MSFT", "GOOG"],
            "entry_date": pd.to_datetime(["1982-11-30", "1994-06-01", "2006-04-03"]),
            "exit_date": pd.to_datetime([pd.NaT, pd.NaT, pd.NaT]),
        })
        ticker_cik = pd.DataFrame({"ticker": [TICKER], "cik": [CIK]})
        return PointInTimeLookup(reader, membership, ticker_cik), queries

    def test_price_queries_do_not_scale_with_universe_size(self, counting_lookup):
        """
        The N+1 guard. Price lookups must be batched, so the count of
        price-table queries stays flat as the universe grows.
        """
        pit, queries = counting_lookup

        pit.get_universe(date(2024, 1, 5))
        price_queries = [q for q in queries if "FROM prices" in q or "prices" in q]

        assert len(price_queries) <= 3, (
            f"get_universe issued {len(price_queries)} price queries for a "
            "3-name universe — batching has regressed to one query per ticker"
        )

    def test_delisting_scan_is_cached_across_calls(self, counting_lookup):
        """The full-scan MAX(date) must run once per instance, not per ticker."""
        pit, queries = counting_lookup

        pit.get_universe(date(2024, 1, 5))
        pit.get_universe(date(2024, 1, 8))

        scans = [q for q in queries if "MAX(date)" in q]
        assert len(scans) == 1, f"MAX(date) scan ran {len(scans)} times, expected 1"

    def test_batched_and_single_lookups_agree(self, counting_lookup):
        """
        get_universe() and get() must return identical snapshots — they now take
        different paths to the price row, and a divergence there would be a
        silent data bug rather than a crash.
        """
        pit, _ = counting_lookup

        batched = {s.ticker: s for s in pit.get_universe(date(2024, 1, 5))}
        for ticker in batched:
            single = pit.get(ticker, date(2024, 1, 5))
            assert batched[ticker] == single, f"{ticker} differs between paths"

    def test_batched_lookup_respects_the_weekend_fallback(self, counting_lookup):
        """Batching must not lose the last-trading-day semantics."""
        pit, _ = counting_lookup

        snaps = {s.ticker: s for s in pit.get_universe(date(2024, 1, 6))}

        assert snaps[TICKER].close == pytest.approx(185.0), "Must use Friday"
        assert not snaps[TICKER].is_trading_day
