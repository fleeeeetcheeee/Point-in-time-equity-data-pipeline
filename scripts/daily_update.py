"""
Incremental daily update.

Refreshes only data that may have changed since the last run:
  - Today's prices for all tickers in the processed store
  - Any new EDGAR filings from the current quarter
  - Membership changes (re-scrapes Wikipedia)

Intended to be run as a cron job after market close (e.g. 6pm ET).

Usage
-----
    export SEC_USER_AGENT="Your Name your@email.com"
    python scripts/daily_update.py
"""

from __future__ import annotations

import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tierzero.config import config
from tierzero.ingestion.edgar import EdgarDownloader
from tierzero.ingestion.prices import PriceDownloader
from tierzero.ingestion.sp500_membership import SP500MembershipScraper
from tierzero.processing.edgar_parser import EdgarParser
from tierzero.processing.membership_builder import MembershipBuilder
from tierzero.processing.price_cleaner import PriceCleaner
from tierzero.storage.writer import ParquetWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("daily_update")


def update_prices() -> None:
    """Refresh prices for all tickers already in the processed store."""
    processed_dir = config.processed_prices_dir

    # Collect tickers from existing partitions
    tickers: set[str] = set()
    if processed_dir.exists():
        for parquet_file in processed_dir.glob("**/data.parquet"):
            try:
                df = pl.read_parquet(parquet_file, columns=["ticker"])
                tickers.update(df["ticker"].unique().to_list())
            except Exception:
                pass

    if not tickers:
        log.warning("No tickers found in processed store. Run bootstrap.py first.")
        return

    log.info("Updating prices for %d tickers…", len(tickers))

    price_dl = PriceDownloader(
        output_dir=config.raw_prices_dir,
        start_date=str(date.today()),  # only fetch from today onward
        batch_size=config.yfinance_batch_size,
    )
    cleaner = PriceCleaner()
    writer = ParquetWriter()

    # Collected and written once, for the same reason bootstrap batches: every
    # ticker updated today lands in the same date partition, so a per-ticker
    # write would leave only the last one. The fixed part name keeps a second
    # run on the same day an overwrite rather than a duplicate.
    pending: list[pl.DataFrame] = []

    for ticker in sorted(tickers):
        try:
            ok = price_dl.download_ticker(ticker)
            if not ok:
                continue

            raw_df = price_dl.load_ohlcv(ticker)
            splits_df = price_dl.load_splits(ticker)
            if raw_df.empty:
                continue

            # Only process today's row
            today = pd.Timestamp(date.today())
            raw_today = raw_df[raw_df.index >= today]
            if raw_today.empty:
                continue

            cleaned = cleaner.to_processed_df(raw_today, splits_df, ticker)
            if cleaned.empty:
                continue

            cleaned = cleaned.reset_index().rename(columns={"index": "date"})
            cleaned["date"] = pd.to_datetime(cleaned["date"]).dt.date
            pl_df = pl.from_pandas(
                cleaned[["date", "ticker", "open", "high", "low",
                          "close_unadj", "close_adj", "volume",
                          "volume_zero", "price_gap_flag"]].dropna(subset=["close_unadj"])
            )
            pending.append(pl_df)

        except Exception as exc:
            log.error("Failed to update %s: %s", ticker, exc)

    if pending:
        writer.write_prices(
            pl.concat(pending), config.processed_prices_dir, part_name="daily"
        )
    else:
        log.warning("No price rows to write — market holiday, or data not yet posted.")


def update_edgar() -> None:
    """Download and parse the current quarter's EDGAR data."""
    today = date.today()
    current_quarter = (today.month - 1) // 3 + 1

    downloader = EdgarDownloader(
        output_dir=config.raw_edgar_dir,
        user_agent=config.edgar_user_agent,
        rate_limit_rps=config.edgar_rate_limit_rps,
    )

    log.info("Refreshing EDGAR %dQ%d…", today.year, current_quarter)
    # Force re-download of the current quarter (it's still being updated by SEC)
    dest_dir = config.raw_edgar_dir / f"{today.year}q{current_quarter}"
    if dest_dir.exists():
        import shutil
        shutil.rmtree(dest_dir)

    downloader.download_quarter(today.year, current_quarter)

    parser = EdgarParser()
    # Remove existing parsed file for current quarter so it gets re-parsed
    out_path = config.processed_fundamentals_dir / f"{today.year}q{current_quarter}.parquet"
    if out_path.exists():
        out_path.unlink()

    parser.parse_all_quarters(config.raw_edgar_dir, config.processed_fundamentals_dir)


def update_membership() -> None:
    """Re-scrape Wikipedia and rebuild the membership timeline."""
    log.info("Refreshing S&P 500 membership timeline…")
    scraper = SP500MembershipScraper(output_dir=config.raw_membership_dir)
    changes = scraper.scrape_wikipedia(force=True)
    builder = MembershipBuilder()
    timeline = builder.build_timeline(changes)
    builder.save(timeline, config.processed_membership_path)
    log.info("  Membership timeline updated: %d records.", len(timeline))


def main() -> None:
    config.require_real_user_agent()
    log.info("Starting daily update for %s", date.today())
    update_prices()
    update_edgar()
    update_membership()
    log.info("Daily update complete.")


if __name__ == "__main__":
    main()
