"""
Tests for the yfinance price/corporate-action downloader.

yfinance is stubbed rather than mocked at the HTTP layer: it does not use
`requests` internally, and pinning its wire format would test Yahoo's API
rather than our client. What matters here is the contract this project depends
on — above all that history is fetched with ``auto_adjust=False``.

That flag is the entire reason the storage layer is trustworthy. With
auto_adjust=True, yfinance retroactively rewrites every historical close when a
new split occurs, so the same (ticker, date) returns different values depending
on when it was fetched. Nothing downstream can detect that has happened, which
is why it is asserted here at the source.
"""

from __future__ import annotations

import pandas as pd
import pytest

from tierzero.ingestion import prices as prices_module
from tierzero.ingestion.prices import PriceDownloader


class FakeTicker:
    """Stand-in for yf.Ticker recording the kwargs it was called with."""

    last_history_kwargs: dict = {}

    def __init__(self, ticker: str, *, empty: bool = False, raises: bool = False):
        self.ticker = ticker
        self._empty = empty
        self._raises = raises

    def history(self, **kwargs):
        FakeTicker.last_history_kwargs = kwargs
        if self._raises:
            raise RuntimeError("Yahoo returned garbage")
        if self._empty:
            return pd.DataFrame()

        # yfinance returns a tz-aware index; the downloader normalises it.
        idx = pd.to_datetime(
            ["2024-01-02", "2024-01-03", "2024-01-04"]
        ).tz_localize("America/New_York")
        return pd.DataFrame(
            {
                "Open": [185.0, 184.0, 182.0],
                "High": [186.0, 185.0, 183.0],
                "Low": [184.0, 183.0, 181.0],
                "Close": [185.5, 184.5, 182.5],
                "Volume": [50_000_000, 48_000_000, 47_000_000],
                "Dividends": [0.0, 0.0, 0.24],
                "Stock Splits": [0.0, 0.0, 0.0],
            },
            index=idx,
        )

    @property
    def splits(self):
        s = pd.Series(
            [7.0, 4.0],
            index=pd.to_datetime(["2014-06-09", "2020-08-31"]).tz_localize("America/New_York"),
            name="Stock Splits",
        )
        s.index.name = "Date"
        return s

    @property
    def dividends(self):
        s = pd.Series(
            [0.24],
            index=pd.to_datetime(["2024-01-04"]).tz_localize("America/New_York"),
            name="Dividends",
        )
        s.index.name = "Date"
        return s


@pytest.fixture
def patch_yf(monkeypatch):
    """Install FakeTicker in place of yf.Ticker, configurable per test."""

    def _install(**kwargs):
        monkeypatch.setattr(
            prices_module.yf, "Ticker", lambda t: FakeTicker(t, **kwargs)
        )

    return _install


@pytest.fixture
def downloader(tmp_path) -> PriceDownloader:
    return PriceDownloader(output_dir=tmp_path, start_date="1993-01-01")


# ======================================================================
# The auto_adjust invariant
# ======================================================================

class TestUnadjustedPrices:

    def test_history_is_fetched_unadjusted(self, downloader, patch_yf):
        """
        auto_adjust=False is a correctness requirement, not a preference —
        see the module docstring.
        """
        patch_yf()
        downloader.download_ticker("AAPL")

        assert FakeTicker.last_history_kwargs["auto_adjust"] is False

    def test_corporate_actions_are_requested(self, downloader, patch_yf):
        """Splits/dividends must come down with the prices, not separately."""
        patch_yf()
        downloader.download_ticker("AAPL")

        assert FakeTicker.last_history_kwargs["actions"] is True

    def test_start_date_is_passed_through(self, downloader, patch_yf):
        patch_yf()
        downloader.download_ticker("AAPL")

        assert FakeTicker.last_history_kwargs["start"] == "1993-01-01"


# ======================================================================
# What lands on disk
# ======================================================================

class TestDownloadTicker:

    def test_writes_ohlcv_splits_and_dividends(self, downloader, patch_yf, tmp_path):
        patch_yf()
        assert downloader.download_ticker("AAPL") is True

        for suffix in ("ohlcv", "splits", "divs"):
            assert (tmp_path / f"AAPL_{suffix}.parquet").exists(), f"missing {suffix}"

    def test_index_is_normalised_to_naive_dates(self, downloader, patch_yf):
        """
        Storage partitions on a plain date. A tz-aware index would push
        late-day US timestamps onto the following UTC day.
        """
        patch_yf()
        downloader.download_ticker("AAPL")

        ohlcv = downloader.load_ohlcv("AAPL")
        assert ohlcv.index.tz is None
        assert ohlcv.index[0] == pd.Timestamp("2024-01-02")

    def test_ticker_column_is_added(self, downloader, patch_yf):
        patch_yf()
        downloader.download_ticker("AAPL")

        assert (downloader.load_ohlcv("AAPL")["ticker"] == "AAPL").all()

    def test_splits_get_the_processing_schema(self, downloader, patch_yf):
        """price_cleaner.compute_adjustment_factors() reads these column names."""
        patch_yf()
        downloader.download_ticker("AAPL")

        splits = downloader.load_splits("AAPL")
        assert list(splits.columns) == ["date", "split_ratio", "ticker"]
        assert splits["split_ratio"].tolist() == [7.0, 4.0]

    def test_empty_history_reports_failure(self, downloader, patch_yf, tmp_path):
        """A delisted or not-yet-listed ticker is normal, not an error."""
        patch_yf(empty=True)

        assert downloader.download_ticker("DEADCO") is False
        assert not (tmp_path / "DEADCO_ohlcv.parquet").exists()

    def test_exception_is_swallowed_into_a_false(self, downloader, patch_yf):
        """
        One bad ticker must not abort a batch of several thousand.
        """
        patch_yf(raises=True)

        assert downloader.download_ticker("BROKEN") is False

    def test_existing_files_are_not_refetched(self, downloader, patch_yf):
        patch_yf()
        downloader.download_ticker("AAPL")
        FakeTicker.last_history_kwargs = {}

        assert downloader.download_ticker("AAPL") is True
        assert FakeTicker.last_history_kwargs == {}, "Should have taken the skip path"


# ======================================================================
# Batch behaviour and read helpers
# ======================================================================

class TestBatchAndLoaders:

    def test_batch_reports_per_ticker_outcomes(self, downloader, monkeypatch):
        monkeypatch.setattr(
            prices_module.time, "sleep", lambda _s: None
        )  # no real delay in tests
        monkeypatch.setattr(
            prices_module.yf,
            "Ticker",
            lambda t: FakeTicker(t, empty=(t == "DEADCO")),
        )

        results = downloader.download_batch(["AAPL", "DEADCO", "MSFT"])

        assert results == {"AAPL": True, "DEADCO": False, "MSFT": True}

    def test_missing_ohlcv_loads_as_empty(self, downloader):
        assert downloader.load_ohlcv("NOSUCH").empty

    def test_missing_splits_keeps_the_expected_columns(self, downloader):
        """
        The cleaner indexes into these columns unconditionally, so the empty
        case must still carry the schema rather than a bare DataFrame.
        """
        splits = downloader.load_splits("NOSUCH")

        assert splits.empty
        assert list(splits.columns) == ["date", "split_ratio", "ticker"]

    def test_missing_dividends_keeps_the_expected_columns(self, downloader):
        divs = downloader.load_divs("NOSUCH")

        assert divs.empty
        assert list(divs.columns) == ["date", "dividend_amount", "ticker"]
