"""
S&P 500 historical constituent timeline builder.

Transforms raw add/remove event data (from Wikipedia scrape) into a clean
(ticker, entry_date, exit_date) timeline that supports point-in-time queries:
  "Was TICKER a member of the S&P 500 on DATE?"
  "Which tickers were in the S&P 500 on DATE?"

Design
------
Wikipedia's changes table records constituent events as:
  (date, ticker_added, ticker_removed)

We process these chronologically, maintaining the set of active members.
Each entry creates a record with (ticker, entry_date); each removal closes
the most recent open record with exit_date. Companies that are added,
removed, and re-added appear as multiple (entry_date, exit_date) rows.

exit_date=NaT means the company is still in the index as of the last known
event. For point-in-time queries, treat NaT as "no exit yet" — the company
was a member from entry_date through the query date.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)


class MembershipBuilder:
    """
    Builds and queries the S&P 500 historical constituent timeline.

    Usage::

        builder = MembershipBuilder()
        timeline = builder.build_timeline(changes_df)
        timeline.to_parquet(output_path, index=False)

        builder.was_member_on(timeline, "AAPL", pd.Timestamp("2020-06-01"))
        builder.get_constituents_on(timeline, pd.Timestamp("2008-09-15"))
    """

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build_timeline(self, changes_df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert a raw changes DataFrame into a (ticker, entry_date, exit_date)
        timeline.

        Parameters
        ----------
        changes_df : DataFrame with columns:
            date             — event date (datetime)
            ticker_added     — ticker symbol added (may be NaN)
            ticker_removed   — ticker symbol removed (may be NaN)

        Returns
        -------
        DataFrame with columns:
            ticker       Utf8
            entry_date   datetime64[ns]
            exit_date    datetime64[ns]  — NaT for current members
        """
        records: list[dict] = []
        # active maps ticker → entry_date for currently open positions
        active: dict[str, pd.Timestamp] = {}

        for _, row in changes_df.sort_values("date").iterrows():
            event_date = pd.Timestamp(row["date"])

            added = row.get("ticker_added")
            if pd.notna(added):
                ticker = str(added).strip()
                if ticker and ticker not in active:
                    active[ticker] = event_date
                elif ticker in active:
                    # Re-entry after a gap — close the old record first
                    # (shouldn't happen in clean data, but guard anyway)
                    pass

            removed = row.get("ticker_removed")
            if pd.notna(removed):
                ticker = str(removed).strip()
                if ticker in active:
                    records.append(
                        {
                            "ticker": ticker,
                            "entry_date": active.pop(ticker),
                            "exit_date": event_date,
                        }
                    )
                else:
                    log.debug(
                        "Removal event for %s on %s but ticker not in active set.",
                        ticker,
                        event_date.date(),
                    )

        # All remaining active members have no exit date yet
        for ticker, entry_date in active.items():
            records.append(
                {"ticker": ticker, "entry_date": entry_date, "exit_date": pd.NaT}
            )

        df = pd.DataFrame(records, columns=["ticker", "entry_date", "exit_date"])
        df["entry_date"] = pd.to_datetime(df["entry_date"])
        df["exit_date"] = pd.to_datetime(df["exit_date"])
        return df.sort_values(["ticker", "entry_date"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # Point-in-time queries
    # ------------------------------------------------------------------

    def was_member_on(
        self,
        timeline: pd.DataFrame,
        ticker: str,
        date: pd.Timestamp,
    ) -> bool:
        """
        Return True if ticker was in the S&P 500 on the given date.

        A ticker is a member on date D if:
          entry_date <= D  AND  (exit_date IS NaT  OR  exit_date > D)
        """
        rows = timeline[timeline["ticker"] == ticker]
        if rows.empty:
            return False

        mask = (rows["entry_date"] <= date) & (
            rows["exit_date"].isna() | (rows["exit_date"] > date)
        )
        return bool(mask.any())

    def get_constituents_on(
        self,
        timeline: pd.DataFrame,
        date: pd.Timestamp,
    ) -> list[str]:
        """
        Return all tickers that were members of the S&P 500 on the given date.
        """
        mask = (timeline["entry_date"] <= date) & (
            timeline["exit_date"].isna() | (timeline["exit_date"] > date)
        )
        return timeline[mask]["ticker"].tolist()

    def get_entry_exit(
        self,
        timeline: pd.DataFrame,
        ticker: str,
        date: pd.Timestamp,
    ) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
        """
        Return (entry_date, exit_date) for the membership period that covers date,
        or (None, None) if the ticker was not a member on that date.
        """
        rows = timeline[timeline["ticker"] == ticker]
        for _, row in rows.iterrows():
            entry = row["entry_date"]
            exit_ = row["exit_date"]
            if entry <= date and (pd.isna(exit_) or exit_ > date):
                return (
                    entry.to_pydatetime() if pd.notna(entry) else None,
                    exit_.to_pydatetime() if pd.notna(exit_) else None,
                )
        return None, None

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    @staticmethod
    def save(timeline: pd.DataFrame, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        timeline.to_parquet(path, index=False)
        log.info("Saved membership timeline (%d rows) to %s", len(timeline), path)

    @staticmethod
    def load(path: Path) -> pd.DataFrame:
        df = pd.read_parquet(path)
        df["entry_date"] = pd.to_datetime(df["entry_date"])
        df["exit_date"] = pd.to_datetime(df["exit_date"])
        return df
