"""
DuckDB-backed reader over the processed Parquet store.

Two persistent SQL views are registered over the Hive-partitioned Parquet files:

  prices        — all daily OHLCV rows, partitioned by date=YYYY-MM-DD/
  fundamentals  — all EDGAR XBRL facts, one file per quarter

Callers use reader.query(sql) to run arbitrary SQL against these views.
The PIT lookup layer (pit/lookup.py) builds all its queries on top of this.

DuckDB reads Parquet files lazily with predicate pushdown, so date-range
filters on the prices view scan only the relevant partition directories
rather than the full dataset.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import polars as pl

log = logging.getLogger(__name__)


class DataReader:
    """
    SQL interface over the processed Parquet store via DuckDB.

    Usage::

        reader = DataReader(data_root)
        df = reader.query("SELECT * FROM prices WHERE ticker = 'AAPL' AND date = '2024-01-15'")
        df = reader.query("SELECT * FROM fundamentals WHERE cik = 320193 AND filed_date <= '2024-01-15'")
    """

    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root
        self._conn = duckdb.connect()
        self._register_views()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def query(self, sql: str) -> pl.DataFrame:
        """Execute a SQL query and return results as a Polars DataFrame."""
        return self._conn.execute(sql).pl()

    def refresh_views(self) -> None:
        """Re-register views — call after new Parquet files are written."""
        self._register_views()

    # ------------------------------------------------------------------
    # View registration
    # ------------------------------------------------------------------

    def _register_views(self) -> None:
        prices_glob = str(
            self.data_root / "processed" / "prices" / "**" / "*.parquet"
        )
        fundamentals_glob = str(
            self.data_root / "processed" / "fundamentals" / "as_filed" / "*.parquet"
        )
        membership_path = str(
            self.data_root / "processed" / "index_membership" / "sp500_timeline.parquet"
        )

        self._conn.execute(f"""
            CREATE OR REPLACE VIEW prices AS
            SELECT *
            FROM read_parquet('{prices_glob}', hive_partitioning = true)
        """)
        log.debug("Registered 'prices' view over %s", prices_glob)

        self._conn.execute(f"""
            CREATE OR REPLACE VIEW fundamentals AS
            SELECT *
            FROM read_parquet('{fundamentals_glob}')
        """)
        log.debug("Registered 'fundamentals' view over %s", fundamentals_glob)

        # Membership is a single file; only register if it exists
        if Path(membership_path).exists():
            self._conn.execute(f"""
                CREATE OR REPLACE VIEW membership AS
                SELECT *
                FROM read_parquet('{membership_path}')
            """)
            log.debug("Registered 'membership' view over %s", membership_path)

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def table_exists(self, view_name: str) -> bool:
        result = self._conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [view_name],
        ).fetchone()
        return result is not None and result[0] > 0

    def price_date_range(self, ticker: str) -> tuple[str | None, str | None]:
        """Return the (first_date, last_date) available for a ticker."""
        result = self._conn.execute(
            "SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR FROM prices WHERE ticker = ?",
            [ticker],
        ).fetchone()
        if result is None:
            return None, None
        return result[0], result[1]
