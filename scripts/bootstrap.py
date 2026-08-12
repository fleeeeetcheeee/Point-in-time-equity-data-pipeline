"""
One-time historical data bootstrap.

Pulls all data from 2000-Q1 to the present and writes the full processed
Parquet store. Designed to be idempotent — safe to re-run; already-downloaded
or already-parsed artifacts are skipped.

Run order
---------
1. Download all EDGAR quarterly XBRL datasets (2000–present)
2. Scrape S&P 500 historical constituent changes from Wikipedia
3. Build the (ticker, entry_date, exit_date) membership timeline
4. Download price history for all ever-members + current constituents
5. Parse EDGAR quarters → processed/fundamentals/as_filed/
6. Clean prices → processed/prices/  (Hive-partitioned by date)
7. Fetch CIK–ticker mapping from SEC

Estimated runtime: 4–8 hours for full history.
Estimated storage: ~50 GB raw, ~15 GB processed.

Usage
-----
    # Set your SEC user agent first (required):
    export SEC_USER_AGENT="Your Name your@email.com"

    python scripts/bootstrap.py
    python scripts/bootstrap.py --start-year 2010  # lighter run for testing
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd
import polars as pl
import requests

# Ensure the src package is importable when running from the project root
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
log = logging.getLogger("bootstrap")

# How many tickers to accumulate before writing a batch of date partitions.
# Trades peak memory against the number of files per partition: at 250, the
# full ~1,100-ticker universe produces about five files per date.
PRICE_WRITE_BATCH = 250


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bootstrap the full historical data store.")
    p.add_argument("--start-year", type=int, default=config.start_year)
    p.add_argument("--start-quarter", type=int, default=config.start_quarter)
    p.add_argument(
        "--end-year", type=int, default=None,
        help="Cap the EDGAR range (inclusive). Defaults to the latest complete quarter."
    )
    p.add_argument(
        "--end-quarter", type=int, default=None,
        help="Cap the EDGAR range (inclusive), used with --end-year."
    )
    p.add_argument(
        "--skip-edgar", action="store_true",
        help="Skip EDGAR download + parse (use existing raw files)."
    )
    p.add_argument(
        "--skip-prices", action="store_true",
        help="Skip price download + clean (use existing raw files)."
    )
    p.add_argument(
        "--max-tickers", type=int, default=None,
        help="Only fetch prices for the first N tickers. For smoke runs — the full "
             "universe is ~1,100 tickers and dominates the wall-clock time."
    )
    return p.parse_args()


def step_edgar(args: argparse.Namespace) -> None:
    if args.skip_edgar:
        log.info("Step 1/5: EDGAR download skipped (--skip-edgar).")
        return

    log.info("Step 1/5: Downloading EDGAR quarterly datasets (%d-Q%d → present)…",
             args.start_year, args.start_quarter)

    downloader = EdgarDownloader(
        output_dir=config.raw_edgar_dir,
        user_agent=config.edgar_user_agent,
        rate_limit_rps=config.edgar_rate_limit_rps,
    )
    quarters = list(downloader.iter_quarters(
        args.start_year, args.start_quarter, args.end_year, args.end_quarter
    ))
    log.info("  %d quarters to check.", len(quarters))

    # One bad quarter must not kill a run that takes hours. Collect failures and
    # report them at the end, so a partial store is still usable and the operator
    # knows exactly which quarters to re-run.
    failed: list[tuple[int, int, str]] = []
    for year, quarter in quarters:
        try:
            downloader.download_quarter(year, quarter)
        except Exception as exc:
            log.error("  EDGAR %dq%d failed: %s", year, quarter, exc)
            failed.append((year, quarter, str(exc)))

    if failed:
        log.warning(
            "  %d of %d quarters failed to download: %s",
            len(failed), len(quarters),
            ", ".join(f"{y}q{q}" for y, q, _ in failed),
        )
        if len(failed) == len(quarters):
            raise SystemExit(
                "Every EDGAR quarter failed to download. This is normally a dead URL "
                "or a blocked User-Agent, not bad luck — fix that before re-running "
                "rather than letting the pipeline build a fundamentals-free store."
            )

    log.info("Step 2/5: Parsing EDGAR quarters → processed fundamentals…")
    parser = EdgarParser()
    parser.parse_all_quarters(config.raw_edgar_dir, config.processed_fundamentals_dir)


def step_membership() -> pd.DataFrame:
    log.info("Step 3/5: Building S&P 500 membership timeline…")

    scraper = SP500MembershipScraper(output_dir=config.raw_membership_dir)
    changes = scraper.scrape_wikipedia()

    builder = MembershipBuilder()
    timeline = builder.build_timeline(changes)
    builder.save(timeline, config.processed_membership_path)

    log.info("  Timeline: %d membership records for %d unique tickers.",
             len(timeline), timeline["ticker"].nunique())
    return timeline


def step_prices(timeline: pd.DataFrame, args: argparse.Namespace) -> None:
    if args.skip_prices:
        log.info("Step 4/5: Price download skipped (--skip-prices).")
        return

    log.info("Step 4/5: Downloading prices for all ever-members…")

    all_tickers = timeline["ticker"].unique().tolist()
    if args.max_tickers is not None:
        all_tickers = all_tickers[: args.max_tickers]
        log.warning(
            "  --max-tickers=%d: fetching a SUBSET of the universe. The resulting "
            "store is a smoke-test artifact, not a backtestable universe.",
            args.max_tickers,
        )
    log.info("  %d unique tickers to fetch.", len(all_tickers))

    price_dl = PriceDownloader(
        output_dir=config.raw_prices_dir,
        start_date=config.prices_start_date,
        batch_size=config.yfinance_batch_size,
    )
    results = price_dl.download_batch(all_tickers)
    failed = [t for t, ok in results.items() if not ok]
    if failed:
        log.warning("  %d tickers had no price data: %s", len(failed), failed[:20])

    log.info("Step 5/5: Cleaning prices and writing processed Parquet…")
    cleaner = PriceCleaner()
    writer = ParquetWriter()

    tickers_with_data = [t for t, ok in results.items() if ok]

    # Tickers are accumulated and flushed in batches rather than written one at
    # a time. A date partition holds every ticker trading that day, so a
    # per-ticker write overwrites the rows every previous ticker put in the
    # partitions they share — the store would end up holding only the last
    # ticker. Batching also keeps the file count per partition small.
    pending: list[pl.DataFrame] = []
    part_index = 0

    def flush() -> None:
        nonlocal pending, part_index
        if not pending:
            return
        writer.write_prices(
            pl.concat(pending), config.processed_prices_dir,
            part_name=f"part-{part_index:04d}",
        )
        pending = []
        part_index += 1

    for ticker in tickers_with_data:
        raw_df = price_dl.load_ohlcv(ticker)
        splits_df = price_dl.load_splits(ticker)

        if raw_df.empty:
            continue

        cleaned = cleaner.to_processed_df(raw_df, splits_df, ticker)
        if cleaned.empty:
            continue

        # Convert to Polars for the writer
        cleaned = cleaned.reset_index().rename(columns={"index": "date"})
        cleaned["date"] = pd.to_datetime(cleaned["date"]).dt.date
        pl_df = pl.from_pandas(cleaned[
            ["date", "ticker", "open", "high", "low",
             "close_unadj", "close_adj", "volume",
             "volume_zero", "price_gap_flag"]
        ].dropna(subset=["close_unadj"]))

        pending.append(pl_df)
        if len(pending) >= PRICE_WRITE_BATCH:
            flush()

    flush()
    log.info("  Price processing complete.")


def step_cik_map() -> None:
    """
    Fetch the SEC company_tickers.json and save as a (cik, ticker, title) Parquet.
    This provides the CIK ↔ ticker mapping needed by PointInTimeLookup.
    """
    log.info("Fetching CIK–ticker map from SEC…")
    out_path = config.data_root / "processed" / "cik_ticker_map.parquet"
    if out_path.exists():
        log.info("  CIK map already exists, skipping.")
        return

    resp = requests.get(
        config.edgar_tickers_url,
        headers={"User-Agent": config.edgar_user_agent},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    rows = [
        {"cik": int(v["cik_str"]), "ticker": v["ticker"], "title": v["title"]}
        for v in data.values()
    ]
    df = pl.DataFrame(rows)
    df.write_parquet(out_path)
    log.info("  Saved %d CIK–ticker mappings to %s", len(df), out_path)


def main() -> None:
    args = parse_args()

    config.require_real_user_agent()

    step_edgar(args)
    timeline = step_membership()
    step_prices(timeline, args)
    step_cik_map()

    log.info("Bootstrap complete. Run pytest to verify data integrity.")


if __name__ == "__main__":
    main()
