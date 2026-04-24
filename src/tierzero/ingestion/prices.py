"""
Price and corporate-action downloader via yfinance.

Design note — why auto_adjust=False
-------------------------------------
yfinance's auto_adjust=True silently back-adjusts *all* historical prices
whenever a new split or dividend occurs. If you build a database with
auto-adjusted prices and a split happens later, the same (ticker, date) row
now returns a different value depending on when you last fetched — i.e. the
store is no longer reproducible.

We store unadjusted closes alongside the raw corporate-actions log. The
point-in-time adjustment factor for any (ticker, as_of_date) is computed in
processing/price_cleaner.py using only the actions known by that date.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Iterable

import pandas as pd
import yfinance as yf

log = logging.getLogger(__name__)


class PriceDownloader:
    """
    Downloads OHLCV history and corporate actions for a list of tickers.

    Artifacts written per ticker (as Parquet in output_dir):
      {ticker}_ohlcv.parquet   — unadjusted OHLCV + Dividends + Stock Splits columns
      {ticker}_splits.parquet  — split events: (date, split_ratio)
      {ticker}_divs.parquet    — dividend events: (date, dividend_amount)

    Usage::

        dl = PriceDownloader(raw_prices_dir)
        dl.download_ticker("AAPL")
        dl.download_batch(["AAPL", "MSFT", "GOOG"])
    """

    def __init__(
        self,
        output_dir: Path,
        start_date: str = "1993-01-01",
        batch_size: int = 50,
    ) -> None:
        self.output_dir = output_dir
        self.start_date = start_date
        self.batch_size = batch_size
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def download_ticker(self, ticker: str) -> bool:
        """
        Download full OHLCV history and corporate actions for one ticker.

        Returns True on success, False if yfinance returned no data
        (e.g. ticker not yet listed, already delisted and no history available).
        """
        ohlcv_path = self.output_dir / f"{ticker}_ohlcv.parquet"
        splits_path = self.output_dir / f"{ticker}_splits.parquet"
        divs_path = self.output_dir / f"{ticker}_divs.parquet"

        if ohlcv_path.exists() and splits_path.exists() and divs_path.exists():
            log.debug("%s already downloaded, skipping.", ticker)
            return True

        try:
            t = yf.Ticker(ticker)

            ohlcv = t.history(
                start=self.start_date,
                auto_adjust=False,  # preserve unadjusted closes — see module docstring
                actions=True,
            )

            if ohlcv.empty:
                log.warning("%s: no price data returned.", ticker)
                return False

            # Normalise index to timezone-naive dates
            ohlcv.index = pd.DatetimeIndex(ohlcv.index).tz_localize(None).normalize()
            ohlcv.index.name = "date"
            ohlcv["ticker"] = ticker

            splits = t.splits.reset_index()
            if not splits.empty:
                splits.columns = ["date", "split_ratio"]
                splits["date"] = pd.to_datetime(splits["date"]).dt.tz_localize(None)
                splits["ticker"] = ticker

            divs = t.dividends.reset_index()
            if not divs.empty:
                divs.columns = ["date", "dividend_amount"]
                divs["date"] = pd.to_datetime(divs["date"]).dt.tz_localize(None)
                divs["ticker"] = ticker

            ohlcv.to_parquet(ohlcv_path)
            (splits if not splits.empty else pd.DataFrame(columns=["date", "split_ratio", "ticker"])).to_parquet(splits_path, index=False)
            (divs if not divs.empty else pd.DataFrame(columns=["date", "dividend_amount", "ticker"])).to_parquet(divs_path, index=False)

            log.info("%s: %d rows saved.", ticker, len(ohlcv))
            return True

        except Exception as exc:
            log.error("%s: download failed — %s", ticker, exc)
            return False

    def download_batch(self, tickers: Iterable[str]) -> dict[str, bool]:
        """
        Download a list of tickers in batches with a short sleep between
        batches to avoid hammering the yfinance / Yahoo Finance endpoint.

        Returns a dict mapping ticker → success flag.
        """
        results: dict[str, bool] = {}
        batch = list(tickers)

        for i in range(0, len(batch), self.batch_size):
            chunk = batch[i : i + self.batch_size]
            log.info(
                "Downloading prices: batch %d/%d (%d tickers)",
                i // self.batch_size + 1,
                -(-len(batch) // self.batch_size),
                len(chunk),
            )
            for ticker in chunk:
                results[ticker] = self.download_ticker(ticker)
            time.sleep(1)

        return results

    # ------------------------------------------------------------------
    # Read helpers (used by processing layer)
    # ------------------------------------------------------------------

    def load_ohlcv(self, ticker: str) -> pd.DataFrame:
        path = self.output_dir / f"{ticker}_ohlcv.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def load_splits(self, ticker: str) -> pd.DataFrame:
        path = self.output_dir / f"{ticker}_splits.parquet"
        if not path.exists():
            return pd.DataFrame(columns=["date", "split_ratio", "ticker"])
        return pd.read_parquet(path)

    def load_divs(self, ticker: str) -> pd.DataFrame:
        path = self.output_dir / f"{ticker}_divs.parquet"
        if not path.exists():
            return pd.DataFrame(columns=["date", "dividend_amount", "ticker"])
        return pd.read_parquet(path)
