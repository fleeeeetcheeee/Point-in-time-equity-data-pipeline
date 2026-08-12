"""
S&P 500 historical constituent data ingestion.

Two complementary sources are used:

1. Wikipedia "Historical components of the S&P 500"
   Records additions and removals with effective dates going back to 1976.
   Scraping this gives us (date, added, removed) rows which we later process
   into a full (ticker, entry_date, exit_date) timeline in
   processing/membership_builder.py.

   This table used to be the second wikitable on "List of S&P 500 companies".
   It was split into its own article, and selecting it by position broke the
   moment that happened — so the table is now located by its *headers*
   ("Added" and "Removed"), which is what actually identifies it.

2. fja05680/sp500 GitHub CSV (cross-check)
   A pre-assembled CSV of historical S&P 500 snapshots from 1996 onward.
   Used as a sanity-check against the Wikipedia scrape.

Neither source is authoritative (CRSP is), but the combination is sufficient
for survivorship-bias-free backtests over the past ~25 years.
"""

from __future__ import annotations

import logging
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from tierzero.config import config

log = logging.getLogger(__name__)

_CHANGES_URL = "https://en.wikipedia.org/wiki/Historical_components_of_the_S%26P_500"
_CONSTITUENTS_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# A US ticker is uppercase alphanumerics with optional dot/hyphen class suffix
# (BRK.B, BF-B). Anything after the first character outside that set is scraping
# debris: the 2013 Allegion row really does come through as "ALLE |", a leaked
# wikitext pipe. Left in, it becomes a ticker yfinance cannot resolve, and the
# company silently drops out of the universe.
_TICKER_PATTERN = r"^([A-Z][A-Z0-9.\-]*)"
_NULL_STRINGS = {"nan", "none", "<na>", "", "-", "—", "n/a"}


def _clean_ticker_column(col: pd.Series) -> pd.Series:
    """Extract the ticker from a scraped cell, or NA if there isn't one."""
    text = col.astype(str).str.strip().str.upper()
    text = text.mask(text.str.lower().isin(_NULL_STRINGS))
    return text.str.extract(_TICKER_PATTERN, expand=False)
_FJA_URL = (
    "https://raw.githubusercontent.com/fja05680/sp500/master/"
    "S%26P%20500%20Historical%20Components%20%26%20Changes.csv"
)


class SP500MembershipScraper:
    """
    Fetches raw S&P 500 constituent change data from public sources.

    Usage::

        scraper = SP500MembershipScraper(raw_membership_dir)
        changes = scraper.scrape_wikipedia()   # DataFrame of add/remove events
        scraper.fetch_fja_snapshot()           # cross-check CSV
    """

    def __init__(self, output_dir: Path, user_agent: str | None = None) -> None:
        self.output_dir = output_dir
        # Wikipedia's user-agent policy, like the SEC's, asks for a real contact
        # address; the same configured identity serves both.
        self.user_agent = user_agent or config.edgar_user_agent
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Table location
    # ------------------------------------------------------------------

    @staticmethod
    def _find_changes_table(soup: BeautifulSoup):
        """
        Locate the additions/removals table by its headers.

        Position is not a stable identifier — this table has already moved once
        (see the module docstring). Its headers are what make it the changes
        table, so that is what we match on.
        """
        for table in soup.find_all("table", class_="wikitable"):
            headers = " ".join(
                th.get_text(" ", strip=True).lower() for th in table.find_all("th")[:12]
            )
            if "added" in headers and "removed" in headers:
                return table

        raise RuntimeError(
            "Wikipedia page structure changed — no wikitable with 'Added' and "
            f"'Removed' headers found at {_CHANGES_URL}. The changes table has "
            "moved or been renamed; locate it and update _CHANGES_URL."
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scrape_wikipedia(self, force: bool = False) -> pd.DataFrame:
        """
        Scrape the constituent-changes table from Wikipedia and save as Parquet.

        Returns a DataFrame with columns:
          date, ticker_added, name_added, ticker_removed, name_removed
        """
        out_path = self.output_dir / "wikipedia_changes.parquet"

        if out_path.exists() and not force:
            log.info("Wikipedia changes already scraped, loading from cache.")
            return pd.read_parquet(out_path)

        log.info("Scraping S&P 500 changes from Wikipedia...")
        resp = requests.get(
            _CHANGES_URL, headers={"User-Agent": self.user_agent}, timeout=30
        )
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        table = self._find_changes_table(soup)

        # pandas 3.0 removed literal-HTML-string support from read_html — a bare
        # string is now interpreted as a path and raises FileNotFoundError.
        df = pd.read_html(StringIO(str(table)))[0]
        df = self._normalise_changes(df)

        df.to_parquet(out_path, index=False)
        log.info("Saved %d change events to %s", len(df), out_path)
        return df

    def fetch_fja_snapshot(self, force: bool = False) -> pd.DataFrame:
        """
        Fetch the pre-built historical constituent CSV from fja05680/sp500.

        Returns a raw DataFrame; columns vary by version of the CSV.
        """
        out_path = self.output_dir / "fja_historical.csv"

        if out_path.exists() and not force:
            log.info("fja05680 snapshot already downloaded, loading from cache.")
            return pd.read_csv(out_path)

        log.info("Fetching fja05680/sp500 historical CSV...")
        resp = requests.get(_FJA_URL, timeout=60)
        resp.raise_for_status()

        out_path.write_bytes(resp.content)
        df = pd.read_csv(out_path)
        log.info("Saved fja05680 snapshot (%d rows) to %s", len(df), out_path)
        return df

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_changes(df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalise the raw Wikipedia changes table into a consistent schema.

        Wikipedia's column names change occasionally; we map them to a
        stable set of names.
        """
        # Flatten multi-level columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [" ".join(str(c) for c in col).strip() for col in df.columns]

        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        # Map to canonical column names — Wikipedia uses various label forms
        rename_map: dict[str, str] = {}
        for col in df.columns:
            if "date" in col:
                rename_map[col] = "date"
            elif "added" in col and "ticker" in col:
                rename_map[col] = "ticker_added"
            elif "added" in col and ("security" in col or "name" in col):
                rename_map[col] = "name_added"
            elif "removed" in col and "ticker" in col:
                rename_map[col] = "ticker_removed"
            elif "removed" in col and ("security" in col or "name" in col):
                rename_map[col] = "name_removed"

        df = df.rename(columns=rename_map)

        # Keep only the canonical columns that are present
        keep = [c for c in ["date", "ticker_added", "name_added", "ticker_removed", "name_removed"] if c in df.columns]
        df = df[keep].copy()

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).reset_index(drop=True)

        for col in ("ticker_added", "ticker_removed"):
            if col in df.columns:
                df[col] = _clean_ticker_column(df[col])

        return df
